from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone = Window.partitionBy("tariff", "zone")
w_all_by_installed_kw = Window.orderBy(F.col("installed_kw").desc(), "id")
w_zone_by_installed_kw_distanc = Window.partitionBy("zone").orderBy(F.col("installed_kw_distance").desc_nulls_first(), "id")
w_all_by_installed_kw_full = Window.orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_installed_kw_dista = Window.partitionBy("tariff").orderBy("installed_kw_distance", "id")
w_all_by_installed_kw_ties = Window.orderBy(F.col("installed_kw").desc())
w_tariff_by_last_installed_kw = Window.partitionBy("tariff").orderBy("last_installed_kw", "id").rowsBetween(-2, -1)
w_tariff_by_last_installed_kw_2 = Window.partitionBy("tariff").orderBy("last_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_tariff = Window.partitionBy("zone", "tariff")
w_zone_next_commissioned_on_mo = Window.partitionBy("zone", F.month("next_commissioned_on")).orderBy(F.col("tariff_zone_max_installed_kw_distance").desc(), "id").rowsBetween(-3, -1)
w_installed_kw_distance_dense = Window.partitionBy("installed_kw_distance_dense_rank")
w_all_by_installed_kw_distance = Window.orderBy("installed_kw_distance_percent_rank", "id")

prepared_sites = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.abs(F.col("installed_kw") - 283.9).alias("installed_kw_distance"),
    )
)

ranked = (
    prepared_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        "installed_kw_distance",
        F.lead("commissioned_on").over(w_all_by_installed_kw).alias("next_commissioned_on"),
        F.dense_rank().over(w_zone_by_installed_kw_distanc).alias("installed_kw_distance_dense_rank"),
        F.last("installed_kw").over(w_all_by_installed_kw_full).alias("last_installed_kw"),
        F.count(F.lit(1)).over(w_tariff_zone).alias("rows_in_tariff_zone"),
        F.round(F.percent_rank().over(w_tariff_by_installed_kw_dista), 3).alias("installed_kw_distance_percent_rank"),
        F.lead("installed_kw_distance", 1, 0).over(w_all_by_installed_kw).alias("next_installed_kw_distance"),
        F.max("installed_kw_distance").over(w_tariff_zone).alias("tariff_zone_max_installed_kw_distance"),
        F.sum("installed_kw_distance").over(w_tariff_zone).alias("tariff_zone_sum_installed_kw_distance"),
        F.rank().over(w_all_by_installed_kw_ties).alias("installed_kw_rank"),
        F.dense_rank().over(w_all_by_installed_kw_ties).alias("installed_kw_dense_rank"),
        F.round(F.col("installed_kw") / F.nullif(F.sum("installed_kw").over(w_tariff_zone), F.lit(0)), 4).alias("installed_kw_share_of_tariff_zone"),
        F.round(F.avg("installed_kw").over(w_tariff_zone), 2).alias("tariff_zone_avg_installed_kw"),
        F.min("installed_kw_distance").over(w_tariff_zone).alias("tariff_zone_min_installed_kw_distance"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round(F.avg("installed_kw_rank").over(w_tariff_by_last_installed_kw), 2).alias("installed_kw_rank_avg_2_prior"),
        F.sum("installed_kw_dense_rank").over(w_tariff_by_last_installed_kw_2).alias("running_installed_kw_dense_rank"),
        F.sum("rows_in_tariff_zone").over(w_zone_tariff).alias("zone_tariff_sum_rows_in_tariff_zone"),
        F.sum("installed_kw_dense_rank").over(w_zone_next_commissioned_on_mo).alias("installed_kw_dense_rank_sum_3_prior"),
        F.round(F.col("installed_kw_distance") - F.avg("installed_kw_distance").over(w_installed_kw_distance_dense), 2).alias("installed_kw_distance_vs_installed_kw_distance_d"),
        F.sum("installed_kw_dense_rank").over(w_zone_tariff).alias("zone_tariff_sum_installed_kw_dense_rank"),
        F.round(F.col("installed_kw_rank") / F.nullif(F.sum("installed_kw_rank").over(w_installed_kw_distance_dense), F.lit(0)), 4).alias("installed_kw_rank_share_of_installed_kw_distance"),
        F.lead("installed_kw_dense_rank").over(w_all_by_installed_kw_distance).alias("next_installed_kw_dense_rank"),
    )
)

rollup_base = (
    with_windows
    .filter(F.col("site_name").isNotNull())
    .groupBy("tariff")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("zone") == "Z4", F.col("installed_kw_dense_rank"))).alias("installed_kw_dense_rank_zone_z4"),
        F.sum(F.when(F.col("site_name").isNotNull(), F.col("running_installed_kw_dense_rank")).otherwise(0)).alias("running_installed_kw_dense_rank_site_name_is_not"),
        F.datediff(F.max("commissioned_on"), F.min("commissioned_on")).alias("commissioned_on_span_days"),
        F.sum("rows_in_tariff_zone").alias("total_rows_in_tariff_zone"),
    )
)

result = (
    rollup_base
    .select(
        "tariff",
        "installed_kw_dense_rank_zone_z4",
        "running_installed_kw_dense_rank_site_name_is_not",
        "commissioned_on_span_days",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
    )
    .orderBy("commissioned_on_span_days", "tariff")
    .limit(20)
)
