from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp")) & (F.col("temp_c") < 25))
    .select("*", F.floor("pressure").alias("pressure_whole"))
)

result = (
    cleaned
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") > 3)
        .select("firmware", "device_code")
    )
)
