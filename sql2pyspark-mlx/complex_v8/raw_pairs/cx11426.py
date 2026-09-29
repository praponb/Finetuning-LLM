from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_distinct_zones_n_length_trim = Window.partitionBy("distinct_zones", "n_length_trim_site_name_7").orderBy(F.col("n_length_trim_site_name_7").desc(), "zone")

prepared = (
    df_sites
    .filter((F.col("tariff") == "TOU") & (F.upper(F.trim(F.col("site_name"))).like("R%")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.when(F.col("installed_kw") >= 284.0, "HIGH").when(F.col("installed_kw") >= 141.6, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
    )
)

totals = (
    prepared
    .groupBy("zone")
    .agg(
        F.count(F.when(F.length(F.trim(F.col("site_name"))) >= 7, 1)).alias("n_length_trim_site_name_7"),
        F.countDistinct("zone").alias("distinct_zones"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.percentile_approx("installed_kw", 0.5).alias("median_installed_kw"),
        F.datediff(F.max("commissioned_on"), F.min("commissioned_on")).alias("commissioned_on_span_days"),
        F.max("installed_kw").alias("max_installed_kw_check"),
    )
    .filter(F.col("max_installed_kw_check") > 93.3)
    .drop("max_installed_kw_check")
)

with_trends = (
    totals
    .select(
        "*",
        F.round(F.col("commissioned_on_span_days") - F.avg("commissioned_on_span_days").over(w_zone), 2).alias("commissioned_on_span_days_vs_zone_avg"),
        F.round(F.cume_dist().over(w_distinct_zones_n_length_trim), 3).alias("n_length_trim_site_name_7_cume_dist"),
        F.round(F.col("n_length_trim_site_name_7") / F.nullif(F.sum("n_length_trim_site_name_7").over(w_zone), F.lit(0)), 4).alias("n_length_trim_site_name_7_share_of_zone"),
        F.min("commissioned_on_span_days").over(w_zone).alias("zone_min_commissioned_on_span_days"),
    )
)

scored = (
    with_trends
    .filter(F.col("zone_min_commissioned_on_span_days") < 4200)
    .select(
        "zone",
        "n_length_trim_site_name_7",
        "distinct_zones",
        "site_name_list",
        "commissioned_on_span_days",
        "commissioned_on_span_days_vs_zone_avg",
        "n_length_trim_site_name_7_cume_dist",
        "n_length_trim_site_name_7_share_of_zone",
        F.abs(F.col("distinct_zones") - 6).alias("distinct_zones_distance"),
        F.substring(F.trim(F.col("zone")), 1, 3).alias("zone_prefix"),
    )
)

annotated = (
    scored
    .filter(F.col("commissioned_on_span_days_vs_zone_avg") >= -490.0)
    .select(
        "*",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        (F.col("distinct_zones") - F.col("distinct_zones_distance")).alias("distinct_zones_minus_distinct_zones_distance"),
    )
)

result = (
    annotated
    .select(
        "zone",
        "distinct_zones",
        "site_name_list",
        "commissioned_on_span_days",
        "commissioned_on_span_days_vs_zone_avg",
        "n_length_trim_site_name_7_cume_dist",
        "distinct_zones_distance",
        "zone_prefix",
        "zone_len",
        "distinct_zones_minus_distinct_zones_distance",
    )
    .orderBy("distinct_zones", F.col("zone_len").desc(), "zone")
    .limit(20)
)
