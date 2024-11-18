use std::sync::Arc;
use std::{fs::File, io::BufReader};
use std::io::Write;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;

fn main() {
    if let Err(e) = run() {
        eprintln!("Application error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    // 定義 TSV 文件的 Schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("conn_hash", DataType::Utf8, false),
        Field::new("stmt_id", DataType::Int32, false),
        Field::new("exec_id", DataType::Int32, false),
        Field::new("exec_time", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("sql_type", DataType::Utf8, true),
        Field::new("exe_status", DataType::Utf8, true),
        Field::new("db_ip", DataType::Utf8, true),
        Field::new("client_ip", DataType::Utf8, true),
        Field::new("client_host", DataType::Utf8, true),
        Field::new("app_name", DataType::Utf8, true),
        Field::new("db_user", DataType::Utf8, true),
        Field::new("sql_hash", DataType::Utf8, true),
        Field::new("from_tbs", DataType::Utf8, true),
        Field::new("select_cols", DataType::Utf8, true),
        Field::new("sql_stmt", DataType::Utf8, true),
        Field::new("stmt_bind_vars", DataType::Utf8, true),
    ]));

    // 開啟 TSV 檔案
    let tsv_file = File::open("sql_logs.tsv")?;
    let tsv_reader = BufReader::new(tsv_file);

    // 使用 Arrow CSV ReaderBuilder
    let mut csv_reader = ReaderBuilder::new(schema.clone())
        .has_header(true)
        .with_delimiter(b'\t')
        .build(tsv_reader)?;

    // 將讀取的 CSV 批次轉存為 Parquet 格式
    let output_file = File::create("sql_logs.parquet")?;
    let mut parquet_writer = ArrowWriter::try_new(output_file, schema, None)?;

    // 累積行數計數器
    let mut total_rows = 0;

    while let Some(batch_result) = csv_reader.next() {
        match batch_result {
            Ok(batch) => {
                total_rows += batch.num_rows();
                // 使用 ANSI 控制碼覆蓋輸出
                print!("\rProcessing rows: {}", total_rows);
                std::io::stdout().flush().unwrap(); // 確保即時輸出
                parquet_writer.write(&batch)?;
            }
            Err(err) => {
                eprintln!("\nError processing batch: {:?}", err);
                return Err(Box::new(err));
            }
        }
    }

    parquet_writer.close()?;
    println!("\nConversion to Parquet completed! Total rows: {}", total_rows);

    Ok(())
}