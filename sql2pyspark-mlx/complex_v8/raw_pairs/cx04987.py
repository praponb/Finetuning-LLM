from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("kwh").between(14.4, 81.9)) & (F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

scored = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_clean",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("OK", "BAD"), 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    with_stats
    .cube("site_id", "quality", "reading_quality_count")
    .agg(
        F.round(F.avg("voltage_filled"), 2).alias("avg_voltage_filled"),
        F.min("reading_quality_count").alias("min_reading_quality_count"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "reading_quality_count",
        "avg_voltage_filled",
        "min_reading_quality_count",
    )
)
