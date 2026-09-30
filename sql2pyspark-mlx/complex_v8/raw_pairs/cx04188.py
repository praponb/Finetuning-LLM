from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_installed_kw = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id")
w_tariff_by_installed_kw_full = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff = Window.partitionBy("tariff")
w_all_by_installed_kw_prior_6 = Window.orderBy(F.col("installed_kw").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_tariff_by_installed_kw_ties = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc())
w_all_by_installed_kw_running = Window.orderBy(F.col("installed_kw").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_installed_kw = Window.orderBy(F.col("installed_kw").desc_nulls_first(), "id")
w_all_by_installed_kw_2 = Window.orderBy("installed_kw", "id")
w_installed_kw_rank = Window.partitionBy("installed_kw_rank")
w_installed_kw_rank_by_first_k = Window.partitionBy("installed_kw_rank").orderBy(F.col("first_known_installed_kw").desc(), "id").rowsBetween(-2, Window.currentRow)
w_installed_kw_rank_by_prev2_i = Window.partitionBy("installed_kw_rank").orderBy(F.col("prev2_installed_kw_2").desc(), "id")
w_installed_kw_rank_by_first_k_2 = Window.partitionBy("installed_kw_rank").orderBy(F.col("first_known_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_sites
    .filter((F.col("installed_kw") <= 284.0) & (F.upper(F.trim(F.col("site_name"))).like("S%")))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
    )
)

metrics = (
    filtered
    .select(
        "id",
        "site_name",
        "installed_kw",
        F.lead("installed_kw", 2).over(w_tariff_by_installed_kw).alias("next2_installed_kw"),
        F.nth_value("installed_kw", 2).over(w_tariff_by_installed_kw_full).alias("second_installed_kw"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.max("installed_kw").over(w_all_by_installed_kw_prior_6).alias("installed_kw_max_6_prior"),
        F.lag("installed_kw", 2).over(w_tariff_by_installed_kw).alias("prev2_installed_kw"),
        F.round(F.percent_rank().over(w_tariff_by_installed_kw_ties), 3).alias("installed_kw_percent_rank"),
        F.first("installed_kw", ignorenulls=True).over(w_all_by_installed_kw_running).alias("first_known_installed_kw"),
        F.sum("installed_kw").over(w_tariff).alias("tariff_sum_installed_kw"),
        F.rank().over(w_all_by_installed_kw).alias("installed_kw_rank"),
        F.first("installed_kw").over(w_tariff_by_installed_kw_full).alias("first_installed_kw"),
        F.lag("installed_kw", 2).over(w_all_by_installed_kw_2).alias("prev2_installed_kw_2"),
    )
)

annotated = (
    metrics
    .filter(F.col("prev2_installed_kw_2") >= 96.9)
    .select(
        "id",
        "site_name",
        "next2_installed_kw",
        "second_installed_kw",
        "installed_kw_max_6_prior",
        "prev2_installed_kw",
        "installed_kw_percent_rank",
        "first_known_installed_kw",
        "tariff_sum_installed_kw",
        "installed_kw_rank",
        "first_installed_kw",
        "prev2_installed_kw_2",
        F.round(F.col("first_known_installed_kw") / F.nullif(F.col("prev2_installed_kw_2"), F.lit(0)), 3).alias("first_known_installed_kw_per_prev2_installed_kw_"),
        F.abs(F.col("rows_in_tariff") - 10).alias("rows_in_tariff_distance"),
        F.substring(F.trim(F.col("site_name")), 1, 2).alias("site_name_prefix"),
    )
)

metrics_2 = (
    annotated
    .select(
        "id",
        "site_name",
        "next2_installed_kw",
        "second_installed_kw",
        "installed_kw_max_6_prior",
        "prev2_installed_kw",
        "installed_kw_percent_rank",
        "first_known_installed_kw",
        "installed_kw_rank",
        "first_installed_kw",
        "prev2_installed_kw_2",
        "first_known_installed_kw_per_prev2_installed_kw_",
        "rows_in_tariff_distance",
        "site_name_prefix",
        F.count(F.lit(1)).over(w_installed_kw_rank).alias("rows_in_installed_kw_rank"),
        F.round(F.col("installed_kw_rank") / F.nullif(F.sum("installed_kw_rank").over(w_installed_kw_rank), F.lit(0)), 4).alias("installed_kw_rank_share_of_installed_kw_rank"),
        F.max("next2_installed_kw").over(w_installed_kw_rank_by_first_k).alias("next2_installed_kw_max_3"),
        F.lag("installed_kw_rank", 1, 0).over(w_installed_kw_rank_by_prev2_i).alias("prev_installed_kw_rank"),
        F.last("first_known_installed_kw").over(w_installed_kw_rank_by_first_k_2).alias("last_first_known_installed_kw"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "second_installed_kw",
        "installed_kw_max_6_prior",
        "prev2_installed_kw",
        "installed_kw_percent_rank",
        "first_known_installed_kw",
        "prev2_installed_kw_2",
        "rows_in_tariff_distance",
        "site_name_prefix",
        "installed_kw_rank_share_of_installed_kw_rank",
        "next2_installed_kw_max_3",
        "prev_installed_kw_rank",
        "last_first_known_installed_kw",
        F.upper(F.trim(F.col("site_name_prefix"))).alias("site_name_prefix_clean"),
    )
    .orderBy(F.col("last_first_known_installed_kw").asc_nulls_last())
)
