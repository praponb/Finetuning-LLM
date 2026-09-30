from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits",
        "firmware",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        F.greatest("temp_c", "humidity").alias("greatest_temp_c_humidity"),
    )
)

result = (
    cleaned
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c") <= 39)
        .select("id", "device_code")
    )
)
