from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")) & (F.col("kwh") >= 37.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 110.5, "HIGH").when(F.col("kwh") >= 37.1, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
)

reading_side = (
    scoped
    .filter((~F.col("kwh_tier").isin("LOW", "HIGH")) & (F.col("quality") != "EST"))
    .select(
        F.col("id").alias("key_id"),
        F.col("read_at").alias("happened_at"),
        "site_id",
    )
)

reading_side_2 = df_meter_readings.select("id", "read_at", "site_id")

result = (
    reading_side
    .union(reading_side_2)
    .distinct()
    .orderBy(F.col("key_id").desc(), "site_id")
)
