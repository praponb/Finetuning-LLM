from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity_per_status_bits_plus_humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last(), "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("humidity") < 39))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("humidity") / F.nullif("status_bits", F.lit(0))).alias("humidity_per_status_bits"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("temp_c") < 34)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("humidity_per_status_bits") + F.col("humidity")).alias("humidity_per_status_bits_plus_humidity"),
        F.upper("device_code").alias("device_code_upper"),
    )
)

latest = (
    valid_device_readings
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
    )
)

latest_device_readings = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "device_code", "temp_c", "humidity", "status_bits", "firmware")
)

result = (
    latest_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-01"])
    .agg(F.avg("temp_c").alias("value_sum"))
    .select("firmware", F.col("DV-02").alias("dv_02"), F.col("DV-01").alias("dv_01"))
)
