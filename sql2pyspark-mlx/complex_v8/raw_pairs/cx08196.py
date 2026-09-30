from pyspark.sql import functions as F

recent = (
    df_sites
    .filter(F.col("site_name").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        F.round(F.col("installed_kw") * 0.9, 2).alias("installed_kw_adj"),
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_counts = (
    recent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

stats = (
    with_counts
    .groupBy("tariff_or_unknown", "reading_count", "zone")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("tariff_or_unknown").alias("distinct_tariff_or_unknowns"),
        F.round(F.avg("installed_kw_adj"), 2).alias("avg_installed_kw_adj"),
        F.sum("reading_count").alias("total_reading_count"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
    )
)

per_label = (
    stats
    .select(
        "tariff_or_unknown",
        "reading_count",
        "zone",
        "cnt",
        "distinct_tariff_or_unknowns",
        "avg_installed_kw_adj",
        "total_reading_count",
        F.explode_outer("zone_list").alias("zone_list_item"),
    )
)

result = (
    per_label
    .select(
        "tariff_or_unknown",
        "reading_count",
        "zone",
        "cnt",
        "distinct_tariff_or_unknowns",
        "avg_installed_kw_adj",
        "total_reading_count",
        "zone_list_item",
    )
    .orderBy(F.col("total_reading_count").asc())
)
