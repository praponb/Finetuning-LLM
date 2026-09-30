from pyspark.sql import functions as F

base = (
    df_sites
    .filter(F.col("zone").isin("Z3", "Z4"))
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
)

summary = (
    base
    .groupBy(
        F.date_format("commissioned_on", "yyyy-MM").alias("commissioned_on_ym"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("site_name"))) >= 6, 1)) / F.count("*"), 1).alias("pct_length_trim_site_name_6"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("site_name"))) >= 4, 1)) / F.count("*"), 1).alias("pct_length_trim_site_name_4"),
        F.sum(F.when(F.col("site_name").isNotNull(), 1).otherwise(0)).alias("n_site_name_is_not_null"),
        F.percentile_approx("installed_kw", 0.5).alias("median_installed_kw"),
        F.max("installed_kw").alias("max_installed_kw"),
    )
)

enriched = (
    summary
    .select(
        "commissioned_on_ym",
        "pct_length_trim_site_name_6",
        "pct_length_trim_site_name_4",
        "n_site_name_is_not_null",
        "median_installed_kw",
        "max_installed_kw",
        F.length(F.trim(F.col("commissioned_on_ym"))).alias("commissioned_on_ym_len"),
        F.length(F.trim(F.col("commissioned_on_ym"))).alias("commissioned_on_ym_len_2"),
        F.abs(F.col("max_installed_kw") - 203.0).alias("max_installed_kw_distance"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("commissioned_on_ym").isNotNull())
    .select(
        "*",
        F.least(F.col("max_installed_kw"), F.col("median_installed_kw")).alias("least_max_installed_kw_median_installed_kw"),
        F.lower(F.regexp_replace(F.trim(F.col("commissioned_on_ym")), " ", "_")).alias("commissioned_on_ym_slug"),
        F.substring(F.trim(F.col("commissioned_on_ym")), 1, 2).alias("commissioned_on_ym_prefix"),
    )
)

tagged = (
    enriched_2
    .filter(F.col("commissioned_on_ym_slug").isNotNull())
    .select(
        "*",
        (F.col("median_installed_kw") - F.col("max_installed_kw")).alias("median_installed_kw_minus_max_installed_kw"),
    )
)

result = (
    tagged
    .select(
        "commissioned_on_ym",
        "pct_length_trim_site_name_6",
        "pct_length_trim_site_name_4",
        "n_site_name_is_not_null",
        "median_installed_kw",
        "max_installed_kw",
        "commissioned_on_ym_len_2",
        "commissioned_on_ym_slug",
        "commissioned_on_ym_prefix",
        "median_installed_kw_minus_max_installed_kw",
        F.abs(F.col("median_installed_kw") - 351.5).alias("median_installed_kw_distance"),
        F.substring(F.trim(F.col("commissioned_on_ym_slug")), 1, 2).alias("commissioned_on_ym_slug_prefix"),
    )
    .orderBy(F.col("commissioned_on_ym_slug_prefix").desc())
)
