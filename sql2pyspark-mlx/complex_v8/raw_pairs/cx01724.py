from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_rows = Window.orderBy("n_rows", "installed_kw_filled_tier")
w_distinct_zones_installed_kw = Window.partitionBy("distinct_zones", "installed_kw_filled_tier")
w_n_rows_installed_kw_filled_t = Window.partitionBy("n_rows", "installed_kw_filled_tier").orderBy("distinct_zones_installed_kw_filled_tier_avg_n_ro")
w_n_rows_distinct_zones = Window.partitionBy("n_rows", "distinct_zones")
w_installed_kw_filled_tier_dis = Window.partitionBy("installed_kw_filled_tier", "distinct_zones")
w_distinct_zones_by_distinct_z = Window.partitionBy("distinct_zones").orderBy(F.col("distinct_zones").desc(), "installed_kw_filled_tier").rowsBetween(-6, Window.currentRow)
w_distinct_zones_by_distinct_z_2 = Window.partitionBy("distinct_zones").orderBy(F.col("distinct_zones").desc())
w_distinct_zones_dense_rank_by = Window.partitionBy("distinct_zones_dense_rank").orderBy("installed_kw_filled_tier_distinct_zones_avg_dist")
w_installed_kw_filled_tier = Window.partitionBy("installed_kw_filled_tier")

valid = (
    df_sites
    .filter((F.col("installed_kw") < 284.0) & (~F.col("tariff").isin("EV", "TOU")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("zone").isin("Z2", "Z3"), "group_a").when(F.col("zone") == "Z4", "group_b").otherwise("other").alias("zone_group"),
        F.initcap(F.trim(F.col("zone"))).alias("zone_clean"),
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
    )
)

rollup_base = (
    valid
    .filter(F.col("tariff").isin("FLAT", "EV"))
    .groupBy(
        F.when(F.col("installed_kw_filled") >= 326.8, "large").when(F.col("installed_kw_filled") >= 128.8, "medium").otherwise("small").alias("installed_kw_filled_tier"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("tariff").isNotNull(), 1)) / F.count("*"), 1).alias("pct_tariff_is_not_null"),
        F.count("*").alias("n_rows"),
        F.countDistinct("zone").alias("distinct_zones"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
    )
)

sequenced = (
    rollup_base
    .select(
        "installed_kw_filled_tier",
        "n_rows",
        "distinct_zones",
        F.lag("pct_tariff_is_not_null").over(w_all_by_n_rows).alias("prev_pct_tariff_is_not_null"),
        F.round(F.avg("n_rows").over(w_distinct_zones_installed_kw), 2).alias("distinct_zones_installed_kw_filled_tier_avg_n_ro"),
        F.round(F.col("pct_tariff_is_not_null") / F.nullif(F.sum("pct_tariff_is_not_null").over(w_distinct_zones_installed_kw), F.lit(0)), 4).alias("pct_tariff_is_not_null_share_of_distinct_zones_i"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.rank().over(w_n_rows_installed_kw_filled_t).alias("distinct_zones_installed_kw_filled_tier_avg_n_ro_2"),
        F.min("distinct_zones").over(w_n_rows_distinct_zones).alias("n_rows_distinct_zones_min_distinct_zones"),
        F.max("distinct_zones").over(w_n_rows_distinct_zones).alias("n_rows_distinct_zones_max_distinct_zones"),
        F.round(F.col("distinct_zones") - F.avg("distinct_zones").over(w_n_rows_distinct_zones), 2).alias("distinct_zones_vs_n_rows_distinct_zones_avg"),
    )
)

sequenced_2 = (
    metrics
    .select(
        "installed_kw_filled_tier",
        "prev_pct_tariff_is_not_null",
        "distinct_zones_installed_kw_filled_tier_avg_n_ro",
        "distinct_zones_vs_n_rows_distinct_zones_avg",
        F.round(F.avg("distinct_zones").over(w_installed_kw_filled_tier_dis), 2).alias("installed_kw_filled_tier_distinct_zones_avg_dist"),
        F.max("distinct_zones").over(w_distinct_zones_by_distinct_z).alias("distinct_zones_max_7"),
        F.dense_rank().over(w_distinct_zones_by_distinct_z_2).alias("distinct_zones_dense_rank"),
    )
)

windowed = (
    sequenced_2
    .select(
        "installed_kw_filled_tier",
        "prev_pct_tariff_is_not_null",
        "distinct_zones_installed_kw_filled_tier_avg_n_ro",
        "distinct_zones_vs_n_rows_distinct_zones_avg",
        "installed_kw_filled_tier_distinct_zones_avg_dist",
        "distinct_zones_max_7",
        "distinct_zones_dense_rank",
        F.round(F.cume_dist().over(w_distinct_zones_dense_rank_by), 3).alias("installed_kw_filled_tier_distinct_zones_avg_dist_2"),
        F.round(F.avg("prev_pct_tariff_is_not_null").over(w_installed_kw_filled_tier), 2).alias("installed_kw_filled_tier_avg_prev_pct_tariff_is_"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.abs(F.col("distinct_zones_max_7") - 16).alias("distinct_zones_max_7_distance"),
    )
)

result = (
    scored
    .select(
        "installed_kw_filled_tier",
        "prev_pct_tariff_is_not_null",
        "distinct_zones_installed_kw_filled_tier_avg_n_ro",
        "distinct_zones_vs_n_rows_distinct_zones_avg",
        "distinct_zones_dense_rank",
        "distinct_zones_max_7_distance",
        F.concat(F.col("installed_kw_filled_tier"), F.lit("#"), F.col("distinct_zones_max_7").cast("string")).alias("installed_kw_filled_tier_ref"),
    )
    .orderBy(
        F.col("distinct_zones_installed_kw_filled_tier_avg_n_ro").desc(),
        F.col("prev_pct_tariff_is_not_null").desc(),
    )
)
