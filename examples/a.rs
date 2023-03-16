use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;

use anyhow::Result;

use encoding_rs::SHIFT_JIS;
use polars::prelude::*;

fn main() -> Result<()> {
    let root = Path::new(".");
    let src_path = root.join("data.csv");

    let mut xs = vec![];
    let mut reader = BufReader::new(File::open(src_path)?);
    reader.read_to_end(&mut xs)?;

    let bytes = from_cp932_to_utf8(&xs);

    let mut df = CsvReader::new(Cursor::new(bytes))
        .has_header(true)
        .finish()?
        .lazy()
        .select([
            col("apple"),
            col("banana"),
            col("cherry"),
        ])
        .collect()?;

    let mut ys = vec![];
    let mut zs = Cursor::new(&mut ys);
    CsvWriter::new(&mut zs).finish(&mut df)?;

    let s = from_utf8_to_cp932(ys);

    let mut out = BufWriter::new(File::create(root.join("out.csv"))?);
    out.write_all(&s)?;
    Ok(())
}

fn from_cp932_to_utf8(xs: &[u8]) -> Vec<u8> {
    let (res, _, _) = SHIFT_JIS.decode(xs);
    //let text = res.to_string();
    //Ok(text.as_bytes().clone())
    res.to_string().into_bytes()
}

fn from_utf8_to_cp932(xs: Vec<u8>) -> Vec<u8> {
    let s = String::from_utf8_lossy(&xs);
    let (jis, _, _) = SHIFT_JIS.encode(&s);
    jis.into_owned()
}
