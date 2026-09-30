from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_commissioned_on = Window.orderBy("commissioned_on", "id")
w_commissioned_on_month_zone_b = Window.partitionBy(F.month("commissioned_on"), "zone").orderBy("installed_kw")
w_tariff = Window.partitionBy("tariff")
w_all_by_installed_kw_bucket_f = Window.orderBy("installed_kw_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_zone = Window.partitionBy("tariff", "zone")
w_all_by_installed_kw = Window.orderBy(F.col("installed_kw").asc_nulls_last(), "id")
w_all_by_commissioned_on_runni = Window.orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_commissioned_on_prior = Window.orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(-1, -1)
w_all_by_commissioned_on_full = Window.orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_installed_kw_rank_commission = Window.partitionBy("installed_kw_rank", F.date_trunc("month", "commissioned_on")).orderBy(F.col("commissioned_on").desc(), "id")
w_all_by_commissioned_on_2 = Window.orderBy(F.col("commissioned_on").desc(), "id")

filtered_sites = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("S%"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

metrics = (
    filtered_sites
    .select(
        "*",
        F.lead("installed_kw", 3).over(w_all_by_commissioned_on).alias("next3_installed_kw"),
        F.rank().over(w_commissioned_on_month_zone_b).alias("installed_kw_rank"),
        F.sum("installed_kw").over(w_tariff).alias("tariff_sum_installed_kw"),
        F.round(F.percent_rank().over(w_all_by_commissioned_on), 3).alias("commissioned_on_percent_rank"),
        F.nth_value("commissioned_on", 2).over(w_all_by_installed_kw_bucket_f).alias("second_commissioned_on"),
        F.count(F.lit(1)).over(w_tariff_zone).alias("rows_in_tariff_zone"),
        F.round(F.percent_rank().over(w_all_by_installed_kw), 3).alias("installed_kw_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_commissioned_on_runni).alias("running_count"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.sum("installed_kw_bucket").over(w_all_by_commissioned_on_prior).alias("installed_kw_bucket_sum_1_prior"),
        F.nth_value("running_count", 2).over(w_all_by_commissioned_on_full).alias("second_running_count"),
        (F.col("commissioned_on_percent_rank") - F.lag("commissioned_on_percent_rank").over(w_installed_kw_rank_commission)).alias("commissioned_on_percent_rank_change"),
        F.round(F.cume_dist().over(w_all_by_commissioned_on_2), 3).alias("commissioned_on_cume_dist"),
        F.row_number().over(w_installed_kw_rank_commission).alias("rn"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
        F.round(F.col("tariff_sum_installed_kw") / F.nullif(F.sum("tariff_sum_installed_kw").over(w_tariff), F.lit(0)), 4).alias("tariff_sum_installed_kw_share_of_tariff"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        "commissioned_on_label",
        "installed_kw_rank",
        "tariff_sum_installed_kw",
        "commissioned_on_percent_rank",
        "second_commissioned_on",
        "rows_in_tariff_zone",
        "installed_kw_percent_rank",
        "running_count",
        "installed_kw_bucket_sum_1_prior",
        "second_running_count",
        "commissioned_on_percent_rank_change",
        "commissioned_on_cume_dist",
        "rn",
        "rows_in_tariff",
        "tariff_sum_installed_kw_share_of_tariff",
        F.least(F.col("installed_kw_rank"), F.col("tariff_sum_installed_kw")).alias("least_installed_kw_rank_tariff_sum_installed_kw"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
    .orderBy(F.col("commissioned_on_label").asc_nulls_last(), "id")
    .limit(10)
)
