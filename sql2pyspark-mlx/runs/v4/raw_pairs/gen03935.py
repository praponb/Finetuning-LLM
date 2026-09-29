from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select("*", F.round("status_bits", 0).alias("status_bits_rounded"))
)

result = (
    valid
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") > 44)
        .select("id", "device_code")
    )
)
