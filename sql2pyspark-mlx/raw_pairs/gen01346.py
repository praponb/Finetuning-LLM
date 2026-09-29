from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.when(F.col("firmware").isin(["1.0.3", "1.1.0"]), "Tier 1").otherwise("Other").alias("firmware_group"),
    )
)

result = (
    filtered
    .select("device_code", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("humidity") > 45)
        .select("device_code", "id")
    )
)
