from pyspark.sql import functions as F

base = df_meter_readings.filter(F.col("voltage") > 215.7)

scored = (
    base
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

enriched_site = (
    scored.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

site_zone_stats = (
    enriched_site
    .groupBy("site_zone")
    .agg(F.percentile_approx("site_installed_kw", 0.5).alias("median_site_installed_kw"))
)

outliers = (
    enriched_site.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.site_installed_kw") >= F.col("g.median_site_installed_kw"))
    .select(
        "s.*",
        "g.median_site_installed_kw",
        F.round(F.col("s.site_installed_kw") - F.col("g.median_site_installed_kw"), 2).alias("site_installed_kw_minus_median"),
    )
)

joined_site = (
    outliers.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_ref",
        "s.read_at_quarter",
        "s.quality_or_unknown",
        "s.site_installed_kw",
        "s.site_commissioned_on",
        "s.site_zone",
        "s.site_installed_kw_minus_median",
        "si.site_name",
    )
)

result = (
    joined_site
    .filter(F.col("site_zone") != "Z4")
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality_or_unknown",
        "site_installed_kw",
        "site_commissioned_on",
        "site_zone",
        "site_installed_kw_minus_median",
        "site_name",
        F.substring(F.trim(F.col("quality_ref")), 1, 3).alias("quality_ref_prefix"),
        F.concat(F.col("site_name"), F.lit("#"), F.col("site_id").cast("string")).alias("site_name_ref"),
    )
    .orderBy(F.col("site_installed_kw_minus_median").desc())
)
