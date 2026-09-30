from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c_plus_humidity").desc_nulls_last(), "id")

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & F.col("pressure").between(1002, 1020))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("temp_c") + F.col("humidity")).alias("temp_c_plus_humidity"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "status_bits", "firmware")
)

filtered = (
    deduped
    .filter(F.col("firmware").isin(["1.1.0", "1.0.3"]))
    .withColumn("status_bits_adjusted", F.round((F.col("status_bits") * 1.1), 2))
)

filtered_device_readings = (
    filtered
    .filter(F.col("status_bits_adjusted") > 50)
    .select(
        "*",
        F.greatest("status_bits_adjusted", "status_bits").alias("greatest_status_bits_adjusted_status_bits"),
    )
)

base_device_readings = (
    filtered_device_readings
    .filter(F.col("status_bits_adjusted") >= 33)
    .withColumn("greatest_status_bits_adjusted_status_bits_plus_status_bits", (F.col("greatest_status_bits_adjusted_status_bits") + F.col("status_bits")))
    .withColumn("status_bits_whole", F.ceil("status_bits"))
)

result = (
    base_device_readings
    .select(
        "id",
        "firmware",
        "status_bits_adjusted",
        "greatest_status_bits_adjusted_status_bits",
        "greatest_status_bits_adjusted_status_bits_plus_status_bits",
        "status_bits_whole",
    )
    .orderBy(F.col("status_bits_adjusted").desc_nulls_first())
)
