from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("firmware").isin(["2.0.1", "1.1.0"]), "Tier 1").otherwise("Other").alias("firmware_group"),
    )
)

result = (
    base
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-02"])
    .agg(F.sum("humidity").alias("amount"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-02").alias("dv_02"))
)
