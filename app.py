import os
import tempfile
import logging
import io
import shutil
import json
from flask import Flask, request, jsonify
from google.cloud import storage, tasks_v2
from PIL import Image, ExifTags
from PIL.Image import Resampling
from PyPDF2 import PdfWriter
import cv2
import numpy as np
from pdf2image import convert_from_path

# Config
DEFAULT_TRACKER_BUCKET_NAME = "migracion-davincci"
TRACKER_FILE = "lista_procesados_completos_mapas.txt"
GCP_PROJECT = "extrac-datos-geosys-production"
GCP_REGION = "us-central1"
CLOUD_TASK_QUEUE = "opt-gis"
WORKER_URL = "https://optimized-gis-386277896892.us-central1.run.app/process_single" 
Image.MAX_IMAGE_PIXELS = None
TARGET_DPI = 80
MAX_FILE_SIZE = 31457280  # 30 MB

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def correct_image_orientation(img):
    try:
        for orientation in ExifTags.TAGS.keys():
            if ExifTags.TAGS[orientation] == 'Orientation':
                break
        exif = img._getexif()
        if exif:
            orientation_value = exif.get(orientation, None)
            if orientation_value == 3:
                img = img.rotate(180, expand=True)
            elif orientation_value == 6:
                img = img.rotate(270, expand=True)
            elif orientation_value == 8:
                img = img.rotate(90, expand=True)
    except Exception:
        pass
    return img

def compress_image_adaptively(input_path, output_path):
    original_size = os.path.getsize(input_path)
    with Image.open(input_path) as img:
        img = correct_image_orientation(img)
        if img.mode != 'RGB':
            img = img.convert('RGB')

        current_megapixels = (img.width * img.height) / 1_000_000
        if original_size > MAX_FILE_SIZE and current_megapixels > 20:
            downscale_factor = (20 / current_megapixels) ** 0.5
            new_width = int(img.width * downscale_factor)
            new_height = int(img.height * downscale_factor)
            img = img.resize((new_width, new_height), Resampling.LANCZOS)

        resize_factor = 1.0
        quality = 95
        min_quality = 90
        min_resize = 0.5

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
            temp_path = temp_file.name

        while True:
            resized_img = img.resize((int(img.width * resize_factor), int(img.height * resize_factor)), Resampling.LANCZOS)
            resized_img.save(temp_path, format="JPEG", dpi=(TARGET_DPI, TARGET_DPI), quality=quality, optimize=True)
            if os.path.getsize(temp_path) <= MAX_FILE_SIZE:
                break
            if quality > min_quality:
                quality -= 5
            elif resize_factor > min_resize:
                resize_factor -= 0.05
            else:
                raise ValueError("No se pudo reducir el archivo sin comprometer la calidad.")

        shutil.copyfile(temp_path, output_path)

def extract_first_page_from_pdf(input_path, output_image_path):
    images = convert_from_path(input_path, first_page=1, last_page=1, dpi=TARGET_DPI)
    images[0].save(output_image_path, 'JPEG')

def split_image_horizontally(image_path, parts=2):
    img = cv2.imread(image_path)
    h, w = img.shape[:2]
    tile_height = h // parts
    tiles = []
    for i in range(parts):
        y1 = i * tile_height
        y2 = h if i == parts - 1 else (i + 1) * tile_height
        tile = img[y1:y2, :]
        tiles.append(tile)
    return tiles

def tiles_to_pdf(tiles, output_pdf_path):
    pdf_writer = PdfWriter()
    for tile in tiles:
        img_pil = Image.fromarray(cv2.cvtColor(tile, cv2.COLOR_BGR2RGB))
        byte_io = io.BytesIO()
        img_pil.save(byte_io, format="PDF")
        byte_io.seek(0)
        pdf_writer.append(byte_io)
    with open(output_pdf_path, "wb") as f_out:
        pdf_writer.write(f_out)

def load_tracker():
    client = storage.Client()
    bucket = client.bucket(DEFAULT_TRACKER_BUCKET_NAME)
    blob = bucket.blob(TRACKER_FILE)
    if not blob.exists():
        return set()
    return set(blob.download_as_text().strip().splitlines())

def update_tracker(new_files, existing):
    all_files = existing.union(new_files)
    client = storage.Client()
    bucket = client.bucket(DEFAULT_TRACKER_BUCKET_NAME)
    blob = bucket.blob(TRACKER_FILE)
    blob.upload_from_string("\n".join(sorted(all_files)))

@app.route("/enqueue_tasks", methods=["POST"])
def enqueue_tasks():
    try:
        data = request.get_json()
        input_bucket_uri = data["input_bucket"]
        output_bucket_uri = data["output_bucket"]
        max_files = int(data.get("max_files", 5))
        horizontal_parts = int(data.get("horizontal_parts", 5))

        input_bucket_name, input_prefix = input_bucket_uri.replace("gs://", "").split("/", 1)
        client = storage.Client()
        blobs = client.bucket(input_bucket_name).list_blobs(prefix=input_prefix)
        processed = load_tracker()
        files = [b.name for b in blobs if b.name not in processed and not b.name.endswith("/")][:max_files]

        task_client = tasks_v2.CloudTasksClient()
        parent = task_client.queue_path(GCP_PROJECT, GCP_REGION, CLOUD_TASK_QUEUE)

        for file_path in files:
            payload = {
                "input_bucket": input_bucket_uri,
                "output_bucket": output_bucket_uri,
                "file_path": file_path,
                "horizontal_parts": horizontal_parts
            }
            task = {
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": WORKER_URL,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(payload).encode()
                }
            }
            task_client.create_task(parent=parent, task=task)

        return jsonify({"tareas_enviadas": len(files)}), 200
    except Exception as e:
        logger.exception("Error en /enqueue_tasks")
        return jsonify({"error": str(e)}), 500

@app.route("/process_single", methods=["POST"])
def process_single():
    try:
        data = request.get_json()
        input_bucket_uri = data["input_bucket"]
        output_bucket_uri = data["output_bucket"]
        file_path = data["file_path"]
        horizontal_parts = int(data.get("horizontal_parts", 2))

        input_bucket_name, _ = input_bucket_uri.replace("gs://", "").split("/", 1)
        output_bucket_name, output_prefix = output_bucket_uri.replace("gs://", "").split("/", 1)

        client = storage.Client()
        input_bucket = client.bucket(input_bucket_name)
        output_bucket = client.bucket(output_bucket_name)
        blob = input_bucket.blob(file_path)
        base_name = os.path.basename(file_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file_path = os.path.join(tmpdir, base_name)
            image_path = os.path.join(tmpdir, "image.jpg")
            final_pdf_path = os.path.join(tmpdir, "final.pdf")

            blob.download_to_filename(local_file_path)
            ext = os.path.splitext(base_name)[1].lower()

            if ext == ".pdf":
                extract_first_page_from_pdf(local_file_path, image_path)
            elif ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff"}:
                compress_image_adaptively(local_file_path, image_path)
            else:
                return jsonify({"skipped": base_name}), 200

            tiles = split_image_horizontally(image_path, parts=horizontal_parts)
            tiles_to_pdf(tiles, final_pdf_path)

            output_blob_path = os.path.join(output_prefix, base_name.rsplit('.', 1)[0] + "_tiles.pdf")
            output_bucket.blob(output_blob_path).upload_from_filename(final_pdf_path)

        update_tracker({file_path}, load_tracker())
        return jsonify({"procesado": file_path}), 200
    except Exception as e:
        logger.exception("Error en /process_single")
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def health():
    return "OK - Horizontal Processor", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
