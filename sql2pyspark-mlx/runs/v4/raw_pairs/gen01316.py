from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure_per_humidity").desc_nulls_first(), "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_timestamp(F.lit("2025-09-01 02:00:00")))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

valid = (
    filtered
    .filter(F.col("pressure") < 1013)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "days_since_reading_ts",
        F.round((F.col("pressure") / F.col("humidity")), 2).alias("pressure_per_humidity"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

valid_device_readings = (
    latest
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "row_num",
        (F.col("status_bits") + F.col("days_since_reading_ts")).alias("status_bits_plus_days_since_reading_ts"),
    )
)

kept_keys = (
    valid_device_readings
    .select("id", "device_code")
    .intersectAll(
        df_device_readings
        .filter(F.col("reading_ts") >= "2025-02-05")
        .select("id", "device_code")
    )
)

kept_rows = (
    valid_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "device_code",
        "temp_c",
        F.col("status_bits_plus_days_since_reading_ts").alias("STATUS_BITS_PLUS_DAYS_SINCE_READING_TS"),
        "humidity",
    )
    .unpivot(
        ["device_code"],
        ["temp_c", "STATUS_BITS_PLUS_DAYS_SINCE_READING_TS", "humidity"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
