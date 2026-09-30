from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w_ordered = Window.orderBy(F.col("temp_c").desc(), "id")
w3 = Window.orderBy(F.col("status_bits").desc(), "id")
w4 = Window.orderBy(F.col("temp_c_rounded").desc())
w5 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("humidity") >= 62)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.substring("firmware", 1, 2).alias("firmware_prefix"),
        F.round("temp_c", 2).alias("temp_c_rounded"),
    )
)

metrics = (
    valid
    .filter(F.col("pressure").between(1015, 1021))
    .select(
        "*",
        F.ntile(4).over(w).alias("status_bits_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(4).over(w3).alias("status_bits_bucket_2"),
        F.rank().over(w4).alias("temp_c_rounded_rank"),
        F.last("temp_c_rounded", ignorenulls=True).over(w5).alias("last_known_temp_c_rounded"),
    )
)

valid_device_readings = (
    metrics
    .filter(F.col("status_bits").between(9, 11))
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "firmware_prefix",
        "temp_c_rounded",
        "status_bits_bucket",
        "seq_no",
        "status_bits_bucket_2",
        (F.col("temp_c_rounded_rank") / F.nullif("temp_c_rounded", F.lit(0))).alias("temp_c_rounded_rank_per_temp_c_rounded"),
    )
)

cleaned = (
    valid_device_readings
    .filter(F.col("status_bits_bucket").between(7, 8))
    .select(
        "*",
        F.round("status_bits_bucket_2", 0).alias("status_bits_bucket_2_rounded"),
        F.when(F.col("status_bits_bucket_2") >= 7, "High").when(F.col("status_bits_bucket_2") >= 4, "Medium").otherwise("Low").alias("status_bits_bucket_2_tier"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "pressure",
        "status_bits",
        "firmware_prefix",
        "temp_c_rounded",
        "status_bits_bucket_2",
        "status_bits_bucket_2_rounded",
        "status_bits_bucket_2_tier",
    )
)
