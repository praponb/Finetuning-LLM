from pyspark.sql import functions as F

prepared = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("H%"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.when(F.col("installed_kw") >= 284.0, "A").when(F.col("installed_kw") >= 141.6, "B").otherwise("C").alias("installed_kw_tier"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

enriched = (
    with_counts
    .filter(F.col("installed_kw_tier").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.when(F.col("installed_kw_tier").isin("C", "A"), "group_a").when(F.col("installed_kw_tier") == "B", "group_b").otherwise("other").alias("installed_kw_tier_group"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("voltage").alias("min_reading_voltage"))
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.min_reading_voltage")
)

result = (
    with_stats
    .groupBy("reading_quality_count", "zone")
    .pivot("zone_or_unknown", ["Z2", "Z3", "UNKNOWN"])
    .agg(F.sum("min_reading_voltage").alias("amount"), F.count("min_reading_voltage").alias("n"))
)
