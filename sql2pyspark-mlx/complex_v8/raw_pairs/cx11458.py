from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_installed_kw_ad = Window.orderBy(F.col("total_installed_kw_adj").asc_nulls_last())
w_all_by_total_installed_kw_ad_2 = Window.orderBy(F.col("total_installed_kw_adj").asc_nulls_last(), "installed_kw_tier", "commissioned_on_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_total_installed_kw_ad_3 = Window.orderBy(F.col("total_installed_kw_adj").asc_nulls_last(), "installed_kw_tier", "commissioned_on_ym")

cleaned = (
    df_sites
    .filter(F.col("zone") == "Z2")
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.round(F.col("installed_kw") * 1.5, 2).alias("installed_kw_adj"),
        F.concat_ws(" / ", F.col("zone"), F.col("tariff")).alias("zone_tariff_label"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.when(F.col("installed_kw") >= 373.3, "large").when(F.col("installed_kw") >= 96.9, "medium").otherwise("small").alias("installed_kw_tier"),
        F.date_format("commissioned_on", "yyyy-MM").alias("commissioned_on_ym"),
    )
    .agg(
        F.min("installed_kw").alias("min_installed_kw"),
        F.round(F.sum("installed_kw_adj"), 2).alias("total_installed_kw_adj"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_total_installed_kw_ad), 3).alias("total_installed_kw_adj_cume_dist"),
        F.dense_rank().over(w_all_by_total_installed_kw_ad).alias("total_installed_kw_adj_dense_rank"),
        F.first("min_installed_kw").over(w_all_by_total_installed_kw_ad_2).alias("first_min_installed_kw"),
        F.lead("min_installed_kw", 3, 0).over(w_all_by_total_installed_kw_ad_3).alias("next3_min_installed_kw"),
        F.lag("total_installed_kw_adj").over(w_all_by_total_installed_kw_ad_3).alias("prev_total_installed_kw_adj"),
        F.row_number().over(w_all_by_total_installed_kw_ad_3).alias("total_installed_kw_adj_seq"),
    )
)

leaders = (
    with_windows
    .filter((F.col("total_installed_kw_adj_seq") == 1) & (F.col("total_installed_kw_adj_cume_dist") > 0.1))
    .drop("total_installed_kw_adj_seq")
)

result = (
    leaders
    .groupingSets(
        [["total_installed_kw_adj_dense_rank", "installed_kw_tier"], ["total_installed_kw_adj_dense_rank"], ["installed_kw_tier"], []],
        "total_installed_kw_adj_dense_rank", "installed_kw_tier",
    )
    .agg(
        F.round(F.sum("first_min_installed_kw"), 2).alias("total_first_min_installed_kw"),
        F.round(F.stddev("prev_total_installed_kw_adj"), 2).alias("stddev_prev_total_installed_kw_adj"),
    )
)
