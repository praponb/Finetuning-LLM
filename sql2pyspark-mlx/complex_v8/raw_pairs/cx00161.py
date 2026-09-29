from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_installed_kw = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id")
w_tariff_by_installed_kw_prior = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(-3, -1)
w_zone = Window.partitionBy("zone")

valid = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("%ro%")) | (F.col("installed_kw") >= 283.9))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "zone_slug",
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_tariff_by_installed_kw)).alias("days_since_prev_commissioned_on"),
        F.max("installed_kw").over(w_tariff_by_installed_kw_prior).alias("installed_kw_max_3_prior"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_zone), 2).alias("installed_kw_vs_zone_avg"),
        F.round(F.cume_dist().over(w_tariff_by_installed_kw), 3).alias("installed_kw_cume_dist"),
        F.sum("installed_kw").over(w_zone).alias("zone_sum_installed_kw"),
    )
)

scored = (
    ranked
    .select(
        "*",
        (F.col("installed_kw_cume_dist") - F.col("installed_kw_max_3_prior")).alias("installed_kw_cume_dist_minus_installed_kw_max_3_"),
        F.when(F.col("zone").isin("Z4", "Z2"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
    )
)

grouped = (
    scored
    .filter(~F.col("zone").isin("Z4", "Z3"))
    .groupBy("tariff", "zone", "zone_group")
    .agg(
        F.min("days_since_prev_commissioned_on").alias("min_days_since_prev_commissioned_on"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("installed_kw_cume_dist_minus_installed_kw_max_3_", 0.5).alias("median_installed_kw_cume_dist_minus_installed_kw"),
        F.max("installed_kw_cume_dist").alias("max_installed_kw_cume_dist_check"),
    )
    .filter(F.col("max_installed_kw_cume_dist_check") > 0.1)
    .drop("max_installed_kw_cume_dist_check")
)

result = (
    grouped
    .filter(F.col("n_rows") >= 4)
    .select(
        "tariff",
        "zone",
        "zone_group",
        "min_days_since_prev_commissioned_on",
        "n_rows",
        "median_installed_kw_cume_dist_minus_installed_kw",
    )
)
