import kagglehub
import boto3
import os


def upload_directory_to_s3(local_dir, bucket_name, s3_prefix="raw"):
    s3_client = boto3.client('s3')

    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            local_file = os.path.join(root, filename)
            s3_key = f"{s3_prefix}/{filename}"

            print(f"Uploading {filename} to s3://{bucket_name}/{s3_key}")
            with open(local_file, 'rb') as file_data:
                s3_client.upload_fileobj(file_data, bucket_name, s3_key)

    print("✅ Upload complete.")


def main():
    dataset_name = "elemento/nyc-yellow-taxi-trip-data"
    local_data_path = kagglehub.dataset_download(dataset_name)

    bucket_name = "nyc-taxi-etl-pipeline-raw-data"
    upload_directory_to_s3(local_data_path, bucket_name)


if __name__ == "__main__":
    main()
