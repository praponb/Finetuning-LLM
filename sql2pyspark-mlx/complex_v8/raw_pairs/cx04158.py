from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").desc(), "id")
w_all_by_commissioned_on_movin = Window.orderBy("commissioned_on", "id").rowsBetween(-3, Window.currentRow)
w_all_by_commissioned_on_2 = Window.orderBy("commissioned_on", "id")
w_all_by_installed_kw_filled = Window.orderBy("installed_kw_filled", "id")
w_all_by_commissioned_on_movin_2 = Window.orderBy("commissioned_on", "id").rowsBetween(-2, Window.currentRow)
w_all_by_commissioned_on_ties = Window.orderBy(F.col("commissioned_on").desc())
w_all_by_installed_kw_filled_p = Window.orderBy("installed_kw_filled", "id").rowsBetween(-2, -1)
w_seq_no_commissioned_on_rank = Window.partitionBy("seq_no", "commissioned_on_rank")
w_seq_no_commissioned_on_month = Window.partitionBy("seq_no", F.date_trunc("month", "commissioned_on")).orderBy("commissioned_on_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_sites = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("S%")) & (F.col("installed_kw") < 373.3))
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
    )
)

with_trends = (
    recent_sites
    .select(
        "*",
        F.lag("installed_kw", 3).over(w_all_by_commissioned_on).alias("prev3_installed_kw"),
        (F.col("installed_kw") - F.lag("installed_kw").over(w_all_by_commissioned_on)).alias("installed_kw_change"),
        F.round(F.avg("installed_kw_filled").over(w_all_by_commissioned_on_movin), 2).alias("installed_kw_filled_avg_4"),
        F.rank().over(w_all_by_commissioned_on_2).alias("commissioned_on_rank"),
        F.row_number().over(w_all_by_installed_kw_filled).alias("seq_no"),
        F.lead("installed_kw_filled").over(w_all_by_installed_kw_filled).alias("next_installed_kw_filled"),
        F.round(F.avg("installed_kw_filled").over(w_all_by_commissioned_on_movin_2), 2).alias("installed_kw_filled_avg_3"),
        F.round(F.percent_rank().over(w_all_by_commissioned_on_ties), 3).alias("commissioned_on_percent_rank"),
        F.max("installed_kw").over(w_all_by_installed_kw_filled_p).alias("installed_kw_max_2_prior"),
        F.sum("installed_kw_filled").over(w_all_by_commissioned_on_movin_2).alias("installed_kw_filled_sum_3"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "site_name",
        "installed_kw",
        "installed_kw_filled",
        "installed_kw_change",
        "installed_kw_filled_avg_4",
        "commissioned_on_rank",
        "seq_no",
        "installed_kw_max_2_prior",
        F.round(F.col("prev3_installed_kw") - F.avg("prev3_installed_kw").over(w_seq_no_commissioned_on_rank), 2).alias("prev3_installed_kw_vs_seq_no_commissioned_on_ran"),
        F.count(F.lit(1)).over(w_seq_no_commissioned_on_rank).alias("rows_in_seq_no_commissioned_on_rank"),
        F.sum("commissioned_on_percent_rank").over(w_seq_no_commissioned_on_month).alias("running_commissioned_on_percent_rank"),
        F.min("installed_kw_filled").over(w_seq_no_commissioned_on_month).alias("min_installed_kw_filled_so_far"),
        F.min("commissioned_on_rank").over(w_seq_no_commissioned_on_rank).alias("seq_no_commissioned_on_rank_min_commissioned_on_"),
    )
)

result = (
    metrics
    .select(
        "id",
        "installed_kw",
        "installed_kw_filled",
        "installed_kw_change",
        "installed_kw_filled_avg_4",
        "commissioned_on_rank",
        "seq_no",
        "installed_kw_max_2_prior",
        "prev3_installed_kw_vs_seq_no_commissioned_on_ran",
        "rows_in_seq_no_commissioned_on_rank",
        "running_commissioned_on_percent_rank",
        "min_installed_kw_filled_so_far",
        F.substring(F.trim(F.col("site_name")), 1, 3).alias("site_name_prefix"),
        (F.floor(F.col("installed_kw_filled") / 100) * 100).alias("installed_kw_filled_bucket"),
    )
    .orderBy("prev3_installed_kw_vs_seq_no_commissioned_on_ran")
)
