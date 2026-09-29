from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("voltage").between(213.5, 234.7)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

rollup_base = (
    filtered
    .groupBy(
        "quality",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
        "site_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("quality") == "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
        F.min("voltage").alias("min_voltage"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
    )
    .filter(F.col("n_rows") >= 3)
)

with_details_site = (
    rollup_base.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("D%")), "inner")
    .select(
        "s.quality",
        "s.read_at_ym",
        "s.site_id",
        "s.n_rows",
        "s.first_read_at",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality") != "EST", 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    with_details_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    with_counts
    .select(
        "quality",
        "read_at_ym",
        "site_id",
        "n_rows",
        "first_read_at",
        "site_zone",
        "site_tariff",
        "reading_count",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_read_at")).alias("days_since_first_read_at"),
    )
    .orderBy(
        F.col("n_rows").desc(),
        "reading_count",
        "quality",
        "read_at_ym",
        "site_id",
    )
    .limit(5)
)
