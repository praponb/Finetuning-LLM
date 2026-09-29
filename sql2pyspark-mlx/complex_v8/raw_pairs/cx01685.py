from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("voltage").between(218.2, 233.6)) & (F.month(F.col("read_at")).isin(1, 2)))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

combined_details_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", "si.site_name")
)

tagged = (
    combined_details_site
    .filter(F.col("kwh_adj").between(45.0, 144.0))
    .select(
        "*",
        F.concat(F.col("quality_ref"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

enriched_site = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 284.0)
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.quality_ref",
        "s.kwh_adj",
        "s.site_name",
        "s.quality_ref_ref",
        "s.days_since_read_at",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

quality_stats = (
    enriched_site
    .groupBy("quality")
    .agg(F.avg("site_installed_kw").alias("avg_site_installed_kw"))
)

outliers = (
    enriched_site.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.site_installed_kw") > F.col("g.avg_site_installed_kw"))
    .select(
        "s.*",
        "g.avg_site_installed_kw",
        F.round(F.col("s.site_installed_kw") - F.col("g.avg_site_installed_kw"), 2).alias("site_installed_kw_minus_avg"),
    )
)

grouped = (
    outliers
    .groupBy("quality")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.length(F.trim(F.col("site_name"))) >= 7, 1).otherwise(0)).alias("n_length_trim_site_name_7"),
        F.round(100 * F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)) / F.count("*"), 1).alias("pct_quality_in_bad_ok"),
    )
)

result = (
    grouped
    .groupBy("n_rows")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.avg("pct_quality_in_bad_ok"))
    .orderBy("n_rows")
)
