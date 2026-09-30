from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") != "OK", 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

qualified = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("voltage").alias("overall_avg_voltage")))
    .filter(F.col("voltage") > F.col("overall_avg_voltage"))
    .drop("overall_avg_voltage")
)

result = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "reading_quality_count",
    )
    .orderBy("id", F.col("read_at").desc_nulls_first())
    .limit(5)
)
