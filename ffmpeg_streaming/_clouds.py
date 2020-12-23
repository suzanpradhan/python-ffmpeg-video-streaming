"""
ffmpeg_streaming.clouds
~~~~~~~~~~~~

Upload and download files -> clouds


:copyright: (c) 2020 by Amin Yazdanpanah.
:website: https://www.aminyazdanpanah.com
:email: contact@aminyazdanpanah.com
:license: MIT, see LICENSE for more details.
"""

import abc
import logging
import tempfile
from os import listdir
from os.path import isfile, join, basename
import os


class Clouds(abc.ABC):
    """
    @TODO: add documentation
    """
    @abc.abstractmethod
    def upload_directory(self, directory: str, **options) -> None:
        pass

    @abc.abstractmethod
    def download(self, filename: str = None, **options) -> str:
        pass


class S3(Clouds):
    def __init__(self, **options):
        """
        @TODO: add documentation
        """
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError as e:
            raise ImportError("No specified import name! make sure that you have installed the package via pip:\n\n"
                              "pip install boto3")

        self.s3 = boto3.client('s3', **options)
        self.err = ClientError

    def upload_directory(self, directory, **options):
        bucket_name = options.pop('bucket_name', None)
        folder = options.pop('folder', '')
        if bucket_name is None:
            raise ValueError('You should pass a bucket name')

        files = [f for f in listdir(directory) if isfile(join(directory, f))]

        try:
            for file in files:
                self.s3.upload_file(join(directory, file), bucket_name, join(folder, file).replace("\\", "/"))
        except self.err as e:
            logging.error(e)
            raise RuntimeError(e)

        logging.info("The {} directory was uploaded to Amazon S3 successfully".format(directory))

    def download(self, filename=None, **options):
        bucket_name = options.pop('bucket_name', None)
        key = options.pop('key', None)

        if bucket_name is None or key is None:
            raise ValueError('You should pass a bucket and key name')

        if filename is None:
            filename = tempfile.NamedTemporaryFile(prefix=basename(key), delete=False)
        else:
            filename = open(filename, 'wb')

        try:
            with filename as f:
                self.s3.download_fileobj(bucket_name, key, f)
            logging.info("The " + filename.name + " file was downloaded")
        except self.err as e:
            logging.error(e)
            raise RuntimeError(e)

        return filename.name

class FS(Clouds):
    bucket = None

    def __init__(self, **options):
        try:
            from google.cloud import storage
            from firebase_admin import credentials, initialize_app, storage
        except ImportError as e:
            raise ImportError("No specified import name! make sure that you have installed the package via pip:\n\n"
                              "pip install google-cloud-storage"
                              "pip install firebase"
                              "pip install firebase_admin")
        if os.getenv("CREDENTIALS_PATH") == None or os.getenv("STORAGE_BUCKET") == None:
            raise ValueError("Make sure you have added CREDENTIALS_PATH and STORAGE_BUCKET in yout os env.")
        cred = credentials.Certificate(os.getenv("CREDENTIALS_PATH"))
        initialize_app(cred, {'storageBucket': os.getenv("STORAGE_BUCKET")})
        FS.bucket = storage.bucket()

    def upload_directory(self, directory, **options):
        folderPath = ''
        folder = options.pop('folder', '')
        if folder != '' or folder != None:
            folderPath = folder + "/"
        files = [f for f in listdir(directory) if isfile(join(directory, f))]
        print("Uploading...")
        for file in files:
            print(file)
            if file.endswith(".m3u8"):
                with open(join(directory, file).replace("\\", "/"), "r+") as m3u8_file_read:
                    lines = m3u8_file_read.readlines()
                    m3u8_file_read.readlines().clear()
                    m3u8_file_read.close()
                    with open(join(directory, file).replace("\\", "/"), "w") as m3u8_file_write:
                        for i in range(len(lines)):
                            if not lines[i].startswith("#"):
                                lines[i] = "https://storage.googleapis.com/" + os.getenv("STORAGE_BUCKET") + "/" + \
                                           folderPath + lines[i].replace("\n", "") + "?alt=media" + "\n"
                        m3u8_file_write.writelines(lines)
                        m3u8_file_write.close()
            blob = FS.bucket.blob(join(folder, file).replace("\\", "/"), **options)
            blob.upload_from_filename(join(directory, file))
            blob.make_public()
        print("Uploaded!")

    def download(self, filename=None, **options):
        object_name = options.pop('object_name', None)

        if object_name is None:
            raise ValueError('You should pass an object name')

        if filename is None:
            with tempfile.NamedTemporaryFile(prefix=basename(object_name), delete=False) as tmp:
                filename = tmp.name

        blob = FS.bucket.get_blob(object_name, **options)
        blob.download_to_filename(filename)
        return filename


class GCS(Clouds):
    CLIENT = None

    def __init__(self, **options):
        """
        @TODO: add documentation
        """
        try:
            from google.cloud import storage
        except ImportError as e:
            raise ImportError("No specified import name! make sure that you have installed the package via pip:\n\n"
                              "pip install google-cloud-storage")
        GCS.CLIENT = storage.Client(**options)

    def upload_directory(self, directory, **options):
        bucket_name = options.pop('bucket_name', None)
        if bucket_name is None:
            raise ValueError('You should pass a bucket name')

        bucket = GCS.CLIENT.get_bucket(bucket_name)
        folder = options.pop('folder', '')
        files = [f for f in listdir(directory) if isfile(join(directory, f))]

        for file in files:
            blob = bucket.blob(join(folder, file).replace("\\", "/"), **options)
            blob.upload_from_filename(join(directory, file))

    def download(self, filename=None, **options):
        bucket_name = options.pop('bucket_name', None)
        if bucket_name is None:
            raise ValueError('You should pass a bucket name')

        bucket = GCS.CLIENT.get_bucket(bucket_name)
        object_name = options.pop('object_name', None)

        if object_name is None:
            raise ValueError('You should pass an object name')

        if filename is None:
            with tempfile.NamedTemporaryFile(prefix=basename(object_name), delete=False) as tmp:
                filename = tmp.name

        blob = bucket.get_blob(object_name, **options)
        blob.download_to_filename(filename)

        return filename


class MAS(Clouds):
    def __init__(self, **options):
        """
        @TODO: add documentation
        """
        try:
            from azure.storage.blob import BlockBlobService
        except ImportError as e:
            raise ImportError("No specified import name! make sure that you have installed the package via pip:\n\n"
                              "pip install azure-storage-blob")
        self.block_blob_service = BlockBlobService(**options)

    def upload_directory(self, directory, **options):
        container = options.pop('container', None)
        if container is None:
            raise ValueError('You should pass a container name')

        files = [f for f in listdir(directory) if isfile(join(directory, f))]

        try:
            for file in files:
                self.block_blob_service.create_blob_from_path(container, file, join(directory, file))
        except:
            error = "An error occurred while uploading the directory"
            logging.error(error)
            raise RuntimeError(error)

    def download(self, filename=None, **options):
        container = options.pop('container', None)
        blob = options.pop('blob', None)

        if container is None or blob is None:
            raise ValueError('You should pass a container name and a blob name')

        if filename is None:
            with tempfile.NamedTemporaryFile(prefix=basename(blob), delete=False) as tmp:
                filename = tmp.name

        try:
            self.block_blob_service.get_blob_to_path(container, blob, filename)
            logging.info("The " + filename + " file was downloaded")
        except:
            error = "An error occurred while downloading the file"
            logging.error(error)
            raise RuntimeError(error)

        return filename


class CloudManager:
    def __init__(self, filename: str = None):
        """
        @TODO: add documentation
        """
        self.filename = filename
        self.clouds = []

    def add(self, cloud: Clouds, **options):
        self.clouds.append((cloud, options))
        return self

    def transfer(self, method, path):
        for cloud in self.clouds:
            getattr(cloud[0], method)(path, **cloud[1])


__all__ = [
    'Clouds',
    'CloudManager',
    'S3',
    'GCS',
    'MAS'
]
