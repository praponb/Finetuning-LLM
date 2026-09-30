from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") >= 222.3) & (F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

reading_rows = (
    cleaned
    .filter(F.col("quality_group").isNotNull())
    .select("id")
)

reading_side = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(F.col("site_id").alias("id"))
)

result = (
    reading_rows
    .intersect(reading_side)
    .orderBy("id")
)
