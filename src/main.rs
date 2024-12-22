use zip::ZipArchive;
use std::error::Error;
use flate2::read::DeflateDecoder;
use serde::{Deserialize, Serialize};
use aws_sdk_s3::{Client, config::Region};
use std::io::{self, Read, Seek, SeekFrom};
use std::fs::{File, OpenOptions,create_dir_all};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};

#[derive(Deserialize, Serialize, Debug)]
struct FileMetadata {
    file_name: String,
    uncompressed_size: u64,
    compressed_size: u64,
    is_directory: bool,
    file_offset: u64,
}

async fn download_bytes(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    byte_range: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let resp = client
        .get_object()
        .bucket(bucket_name)
        .key(object_key)
        .range(byte_range)
        .send()
        .await?;

    let body = resp.body.collect().await?;
    Ok(body.to_vec())
}

async fn get_s3_client() -> Client {
    let s3_endpoint: Option<String> = Some("http://127.0.0.1:9000".to_string());
    let region_provider =
        RegionProviderChain::default_provider().or_else(Region::new("asia-south-1"));
    let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region_provider)
        .load()
        .await;

    let s3_config = if let Some(s3_endpoint) = s3_endpoint {
        aws_sdk_s3::config::Builder::from(&shared_config)
            .endpoint_url(s3_endpoint)
            .force_path_style(true)
            .build()
    } else {
        aws_sdk_s3::config::Builder::from(&shared_config).build()
    };
    aws_sdk_s3::Client::from_conf(s3_config)
}

fn save_central_directory_with_offsets(zip_path: &str, metadata_path: &str) -> io::Result<()> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file)?;

    let file_metadata_list: Vec<FileMetadata> = (0..archive.len())
        .filter_map(|i| {
            let file = archive.by_index(i).ok()?;
            let file_name = file.name().to_string();
            let uncompressed_size = file.size();
            let compressed_size = file.compressed_size();
            let is_directory = file.is_dir();
            let file_offset = file.data_start();
            
            Some(FileMetadata {
                file_name,
                uncompressed_size,
                compressed_size,
                is_directory,
                file_offset,
            })
        })
        .collect();

    let metadata_file = OpenOptions::new().create(true).write(true).open(metadata_path)?;
    serde_cbor::to_writer(metadata_file, &file_metadata_list).unwrap();

    println!("Central Directory with offsets saved to {}", metadata_path);
    Ok(())
}

fn extract_file_from_local_zip(zip_path: &str, file_name: &str, metadata_path: &str) -> io::Result<()> {
    let mut file = File::open(zip_path)?;

    let metadata_file = File::open(metadata_path)?;
    let file_metadata_list: Vec<FileMetadata> = serde_cbor::from_reader(metadata_file).unwrap();

    let metadata = file_metadata_list
        .iter()
        .find(|&meta| meta.file_name == file_name)
        .ok_or(io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

    println!("Found metadata for file: {:?}", metadata);

    let output_file_path = format!("extracted_{}", file_name);
    let output_dir = std::path::Path::new(&output_file_path).parent().unwrap();

    if !output_dir.exists() {
        create_dir_all(output_dir).map_err(|err| {
            io::Error::new(io::ErrorKind::NotFound, format!("Failed to create output directory: {}", err))
        })?;
    }

    let mut output_file = File::create(output_file_path).map_err(|err| {
        io::Error::new(io::ErrorKind::NotFound, format!("Failed to create output file: {}", err))
    })?;

    file.seek(SeekFrom::Start(metadata.file_offset)).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Failed to seek to file offset: {}", err))
    })?;

    let mut compressed_data = file.take(metadata.compressed_size as u64);
    let mut decoder = DeflateDecoder::new(&mut compressed_data);

    io::copy(&mut decoder, &mut output_file).map_err(|err| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to extract file: {}", err))
    })?;

    Ok(())
}

async fn extract_file_from_cloud_zip(client: &Client, zip_path: &str, file_name: &str, metadata_path: &str, bucket_name: &str) -> io::Result<()> {
    let metadata_file = File::open(metadata_path)?;
    let file_metadata_list: Vec<FileMetadata> = serde_cbor::from_reader(metadata_file).unwrap();

    let metadata = file_metadata_list
        .iter()
        .find(|&meta| meta.file_name == file_name)
        .ok_or(io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

    println!("Found metadata for file: {:?}", metadata);

    let output_file_path = format!("extracted_{}", file_name);
    let output_dir = std::path::Path::new(&output_file_path).parent().unwrap();

    if !output_dir.exists() {
        create_dir_all(output_dir).map_err(|err| {
            io::Error::new(io::ErrorKind::NotFound, format!("Failed to create output directory: {}", err))
        })?;
    }

    let mut output_file = File::create(output_file_path).map_err(|err| {
        io::Error::new(io::ErrorKind::NotFound, format!("Failed to create output file: {}", err))
    })?;


    let byte_range = format!("bytes={}-{}", metadata.file_offset as usize, metadata.file_offset + metadata.compressed_size);
    let file = download_bytes(client, bucket_name, zip_path, &byte_range).await.unwrap();

    let mut compressed_data = file.take(metadata.compressed_size as u64);
    let mut decoder = DeflateDecoder::new(&mut compressed_data);

    io::copy(&mut decoder, &mut output_file).map_err(|err| {
        io::Error::new(io::ErrorKind::Other, format!("Failed to extract file: {}", err))
    })?;

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let metadata_path = "local_central_directory_with_offsets.cbor";
    let zip_path = "pc.zip";
    let file_name = "data/r/r2.bin";

    save_central_directory_with_offsets(zip_path, metadata_path)?;
    extract_file_from_local_zip(zip_path, file_name, metadata_path)?;


    let metadata_path = "cloud_central_directory_with_offsets.cbor";
    let target_file_name = "test/RRIF0045_147-2023_F1_140723_062728.JPG";
    let obj_key = "test.zip";

    let client = get_s3_client().await;

    save_central_directory_with_offsets("test.zip", metadata_path)?;
    extract_file_from_cloud_zip(&client, obj_key, target_file_name, metadata_path, "my_bucket").await?;

    Ok(())
}

