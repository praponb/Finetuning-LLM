from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_installed_kw_full = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_installed_kw_bucket_r = Window.orderBy(F.col("installed_kw_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff = Window.partitionBy("tariff")
w_tariff_by_installed_kw_runni = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_installed_kw_bucket = Window.orderBy("installed_kw_bucket", "id")
w_all_by_installed_kw_bucket_2 = Window.orderBy(F.col("installed_kw_bucket").desc(), "id")
w_distinct_zone_in_tariff_cnt = Window.partitionBy("distinct_zone_in_tariff", "cnt").orderBy("cnt", "distinct_zone_in_tariff")
w_distinct_zone_in_tariff_cnt_2 = Window.partitionBy("distinct_zone_in_tariff", "cnt").orderBy("cnt")
w_cnt = Window.partitionBy("cnt")

filtered_sites = (
    df_sites
    .filter(F.col("zone") == "Z4")
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.round(F.col("installed_kw") * 1.1, 2).alias("installed_kw_adj"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

with_trends = (
    filtered_sites
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "installed_kw_adj",
        "installed_kw_bucket",
        F.first("installed_kw").over(w_tariff_by_installed_kw_full).alias("first_installed_kw"),
        F.first("commissioned_on", ignorenulls=True).over(w_all_by_installed_kw_bucket_r).alias("first_known_commissioned_on"),
        F.size(F.collect_set("zone").over(w_tariff)).alias("distinct_zone_in_tariff"),
        F.count(F.lit(1)).over(w_tariff_by_installed_kw_runni).alias("running_count"),
        F.ntile(4).over(w_all_by_installed_kw_bucket).alias("installed_kw_bucket_quartile"),
        F.lag("installed_kw_bucket", 2).over(w_all_by_installed_kw_bucket_2).alias("prev2_installed_kw_bucket"),
    )
)

with_activity = (
    with_trends.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
)

grouped = (
    with_activity
    .groupBy("distinct_zone_in_tariff")
    .agg(
        F.round(100 * F.count(F.when(F.col("zone") == "Z4", 1)) / F.count("*"), 1).alias("pct_zone_z4"),
        F.count("*").alias("cnt"),
    )
)

with_windows = (
    grouped
    .select(
        "distinct_zone_in_tariff",
        "pct_zone_z4",
        F.dense_rank().over(w_distinct_zone_in_tariff_cnt).alias("cnt_dense_rank"),
        F.lead("distinct_zone_in_tariff", 2).over(w_distinct_zone_in_tariff_cnt).alias("next2_distinct_zone_in_tariff"),
        F.dense_rank().over(w_distinct_zone_in_tariff_cnt_2).alias("cnt_dense_rank_2"),
        F.round(F.col("pct_zone_z4") / F.nullif(F.sum("pct_zone_z4").over(w_cnt), F.lit(0)), 4).alias("pct_zone_z4_share_of_cnt"),
        F.round(F.col("distinct_zone_in_tariff") - F.avg("distinct_zone_in_tariff").over(w_cnt), 2).alias("distinct_zone_in_tariff_vs_cnt_avg"),
        F.row_number().over(w_distinct_zone_in_tariff_cnt).alias("cnt_seq"),
    )
)

labeled = (
    with_windows
    .filter(F.col("distinct_zone_in_tariff_vs_cnt_avg") <= 7.0)
    .select(
        "distinct_zone_in_tariff",
        "pct_zone_z4",
        "cnt_dense_rank",
        "next2_distinct_zone_in_tariff",
        "cnt_dense_rank_2",
        "pct_zone_z4_share_of_cnt",
        F.abs(F.col("cnt_dense_rank_2") - 7).alias("cnt_dense_rank_2_distance"),
    )
)

result = (
    labeled
    .filter(F.col("cnt_dense_rank").between(4, 8))
    .select(
        "distinct_zone_in_tariff",
        "pct_zone_z4",
        "cnt_dense_rank_2",
        "pct_zone_z4_share_of_cnt",
        "cnt_dense_rank_2_distance",
        F.abs(F.col("pct_zone_z4") - 45.0).alias("pct_zone_z4_distance"),
    )
    .orderBy(F.col("distinct_zone_in_tariff").asc_nulls_last())
)
