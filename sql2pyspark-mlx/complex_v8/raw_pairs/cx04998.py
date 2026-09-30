from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_commissioned_on_year = Window.partitionBy("tariff", F.year("commissioned_on")).orderBy("installed_kw", "id").rowsBetween(-6, -1)
w_zone_by_installed_kw = Window.partitionBy("zone").orderBy("installed_kw", "id")
w_zone_tariff = Window.partitionBy("zone", "tariff")
w_zone_by_installed_kw_moving = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(-6, Window.currentRow)
w_zone_by_installed_kw_2 = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), "id")
w_zone_by_installed_kw_ties = Window.partitionBy("zone").orderBy("installed_kw")
w_zone_by_installed_kw_full = Window.partitionBy("zone").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_by_installed_kw_prior_3 = Window.partitionBy("zone").orderBy("installed_kw", "id").rowsBetween(-3, -1)
w_tariff_commissioned_on_year_2 = Window.partitionBy("tariff", F.year("commissioned_on")).orderBy("installed_kw")
w_zone_by_installed_kw_running = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone = Window.partitionBy("zone")
w_seq_no_installed_kw_rank_by = Window.partitionBy("seq_no", "installed_kw_rank").orderBy(F.col("installed_kw_vs_zone_tariff_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_by_rows_in_zone_tarif = Window.partitionBy("tariff").orderBy(F.col("rows_in_zone_tariff").desc(), "id")
w_tariff_by_installed_kw_chang = Window.partitionBy("tariff").orderBy(F.col("installed_kw_change").asc_nulls_last())
w_seq_no_installed_kw_rank_by_2 = Window.partitionBy("seq_no", "installed_kw_rank").orderBy(F.col("installed_kw_vs_zone_tariff_avg").desc(), "id")

base = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date")))
)

metrics = (
    base
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.max("installed_kw").over(w_tariff_commissioned_on_year).alias("installed_kw_max_6_prior"),
        F.lag("installed_kw").over(w_zone_by_installed_kw).alias("prev_installed_kw"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_zone_tariff), 2).alias("installed_kw_vs_zone_tariff_avg"),
        F.round(F.avg("installed_kw").over(w_zone_by_installed_kw_moving), 2).alias("installed_kw_avg_7"),
        (F.col("installed_kw") - F.lag("installed_kw").over(w_zone_by_installed_kw_2)).alias("installed_kw_change"),
        F.round(F.cume_dist().over(w_zone_by_installed_kw_ties), 3).alias("installed_kw_cume_dist"),
        F.row_number().over(w_zone_by_installed_kw).alias("seq_no"),
        F.last("commissioned_on").over(w_zone_by_installed_kw_full).alias("last_commissioned_on"),
        F.count(F.lit(1)).over(w_zone_tariff).alias("rows_in_zone_tariff"),
        F.sum("installed_kw").over(w_zone_by_installed_kw_prior_3).alias("installed_kw_sum_3_prior"),
        F.rank().over(w_tariff_commissioned_on_year_2).alias("installed_kw_rank"),
        F.min("installed_kw").over(w_zone_by_installed_kw_running).alias("min_installed_kw_so_far"),
        F.rank().over(w_zone_by_installed_kw_ties).alias("installed_kw_rank_2"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.avg("installed_kw_rank_2").over(w_zone), 2).alias("zone_avg_installed_kw_rank_2"),
        F.first("installed_kw").over(w_seq_no_installed_kw_rank_by).alias("first_installed_kw"),
        F.rank().over(w_tariff_by_rows_in_zone_tarif).alias("rows_in_zone_tariff_rank"),
        F.round(F.percent_rank().over(w_tariff_by_installed_kw_chang), 3).alias("installed_kw_change_percent_rank"),
        F.round(F.cume_dist().over(w_seq_no_installed_kw_rank_by_2), 3).alias("installed_kw_vs_zone_tariff_avg_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_zone_tariff_rank") < 9)
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "prev_installed_kw",
        "installed_kw_vs_zone_tariff_avg",
        "installed_kw_avg_7",
        "installed_kw_change",
        "installed_kw_cume_dist",
        "last_commissioned_on",
        "installed_kw_sum_3_prior",
        "installed_kw_rank",
        "min_installed_kw_so_far",
        "installed_kw_rank_2",
        "first_installed_kw",
        "rows_in_zone_tariff_rank",
        "installed_kw_change_percent_rank",
        "installed_kw_vs_zone_tariff_avg_cume_dist",
    )
    .orderBy(F.col("installed_kw_change_percent_rank").desc(), "id")
    .limit(10)
)
