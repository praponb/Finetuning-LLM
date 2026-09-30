from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-18 07:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

qualified = (
    cleaned
    .crossJoin(cleaned.agg(F.min("voltage").alias("overall_min_voltage")))
    .filter(F.col("voltage") > F.col("overall_min_voltage"))
    .drop("overall_min_voltage")
)

result = (
    qualified
    .groupBy("site_id", "read_at")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.avg("voltage").alias("avg_v"), F.sum("voltage").alias("sum_v"))
)
