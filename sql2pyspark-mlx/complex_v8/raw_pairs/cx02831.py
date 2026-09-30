from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_tariff = Window.partitionBy(F.month("commissioned_on"), "tariff_group")
w_tariff_group_commissioned_on = Window.partitionBy("tariff_group", F.date_trunc("month", "commissioned_on")).orderBy(F.col("installed_kw").desc(), "id")
w_tariff_group_commissioned_on_2 = Window.partitionBy("tariff_group", F.date_trunc("month", "commissioned_on")).orderBy(F.col("installed_kw").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_installed_kw_full = Window.orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_commissioned_on_month = Window.orderBy("commissioned_on_month_tariff_group_max_installed", "id")
w_all_by_commissioned_on_month_2 = Window.orderBy("commissioned_on_month_tariff_group_sum_installed", "id")
w_all_by_commissioned_on_month_3 = Window.orderBy("commissioned_on_month_tariff_group_sum_installed", "id").rowsBetween(-2, Window.currentRow)
w_all_by_commissioned_on_month_4 = Window.orderBy("commissioned_on_month_tariff_group_sum_installed", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_month_tariff_2 = Window.partitionBy("commissioned_on_month_tariff_group_sum_installed_2", "commissioned_on_month_tariff_group_sum_installed_3")
w_commissioned_on_month_tariff_3 = Window.partitionBy("commissioned_on_month_tariff_group_sum_installed_3", "commissioned_on_month_tariff_group_sum_installed_2").orderBy(F.col("installed_kw_sum_4_change").desc(), "id")
w_commissioned_on_month_tariff_4 = Window.partitionBy("commissioned_on_month_tariff_group_sum_installed_2").orderBy("next3_installed_kw_sum_3", "id")
w_commissioned_on_month_tariff_5 = Window.partitionBy("commissioned_on_month_tariff_group_sum_installed_2")
w_commissioned_on_month_tariff_6 = Window.partitionBy("commissioned_on_month_tariff_group_sum_installed_2").orderBy("next3_installed_kw_sum_3")

valid_sites = (
    df_sites
    .filter(F.col("installed_kw").between(93.3, 284.0))
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
        F.when(F.col("tariff").isin("TOU", "FLAT"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group"),
    )
)

ranked = (
    valid_sites
    .select(
        "id",
        "site_name",
        "installed_kw",
        "zone_ref",
        F.max("installed_kw").over(w_commissioned_on_month_tariff).alias("commissioned_on_month_tariff_group_max_installed"),
        F.lead("installed_kw", 3).over(w_tariff_group_commissioned_on).alias("next3_installed_kw"),
        F.round(F.avg("installed_kw").over(w_commissioned_on_month_tariff), 2).alias("commissioned_on_month_tariff_group_avg_installed"),
        F.sum("installed_kw").over(w_tariff_group_commissioned_on_2).alias("installed_kw_sum_4"),
        F.sum("installed_kw").over(w_commissioned_on_month_tariff).alias("commissioned_on_month_tariff_group_sum_installed"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.nth_value("installed_kw_sum_4", 2).over(w_all_by_installed_kw_full).alias("second_installed_kw_sum_4"),
        (F.col("installed_kw_sum_4") - F.lag("installed_kw_sum_4").over(w_all_by_commissioned_on_month)).alias("installed_kw_sum_4_change"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "site_name",
        "installed_kw",
        "zone_ref",
        "commissioned_on_month_tariff_group_max_installed",
        "next3_installed_kw",
        "commissioned_on_month_tariff_group_avg_installed",
        "installed_kw_sum_4",
        "commissioned_on_month_tariff_group_sum_installed",
        "installed_kw_sum_4_change",
        F.rank().over(w_all_by_commissioned_on_month_2).alias("commissioned_on_month_tariff_group_sum_installed_2"),
        F.sum("next3_installed_kw").over(w_all_by_commissioned_on_month_3).alias("next3_installed_kw_sum_3"),
        F.ntile(4).over(w_all_by_commissioned_on_month_2).alias("commissioned_on_month_tariff_group_sum_installed_3"),
        F.first("installed_kw_sum_4", ignorenulls=True).over(w_all_by_commissioned_on_month_4).alias("first_known_installed_kw_sum_4"),
        F.lag("installed_kw").over(w_all_by_commissioned_on_month_2).alias("prev_installed_kw"),
        F.last("installed_kw_sum_4", ignorenulls=True).over(w_all_by_commissioned_on_month_4).alias("last_known_installed_kw_sum_4"),
    )
)

sequenced_2 = (
    with_trends
    .select(
        "id",
        "installed_kw",
        "commissioned_on_month_tariff_group_max_installed",
        "commissioned_on_month_tariff_group_sum_installed",
        "commissioned_on_month_tariff_group_sum_installed_2",
        "next3_installed_kw_sum_3",
        "prev_installed_kw",
        "last_known_installed_kw_sum_4",
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_commissioned_on_month_tariff_2), 2).alias("installed_kw_vs_commissioned_on_month_tariff_gro"),
        F.max("installed_kw_sum_4").over(w_commissioned_on_month_tariff_2).alias("commissioned_on_month_tariff_group_sum_installed_4"),
        F.lead("prev_installed_kw").over(w_commissioned_on_month_tariff_3).alias("next_prev_installed_kw"),
        F.round(F.avg("next3_installed_kw_sum_3").over(w_commissioned_on_month_tariff_2), 2).alias("commissioned_on_month_tariff_group_sum_installed_5"),
        F.lag("installed_kw_sum_4", 1, 0).over(w_commissioned_on_month_tariff_3).alias("prev_installed_kw_sum_4"),
        F.round(F.cume_dist().over(w_commissioned_on_month_tariff_3), 3).alias("installed_kw_sum_4_change_cume_dist"),
    )
)

metrics = (
    sequenced_2
    .select(
        "id",
        "commissioned_on_month_tariff_group_sum_installed",
        "next3_installed_kw_sum_3",
        "prev_installed_kw",
        "last_known_installed_kw_sum_4",
        "installed_kw_vs_commissioned_on_month_tariff_gro",
        "next_prev_installed_kw",
        "commissioned_on_month_tariff_group_sum_installed_5",
        F.row_number().over(w_commissioned_on_month_tariff_4).alias("next3_installed_kw_sum_3_seq"),
        F.round(F.avg("installed_kw").over(w_commissioned_on_month_tariff_5), 2).alias("commissioned_on_month_tariff_group_sum_installed_3"),
        F.max("next3_installed_kw_sum_3").over(w_commissioned_on_month_tariff_5).alias("commissioned_on_month_tariff_group_sum_installed_6"),
        F.dense_rank().over(w_commissioned_on_month_tariff_6).alias("next3_installed_kw_sum_3_dense_rank"),
    )
)

annotated = (
    metrics
    .filter(F.col("next3_installed_kw_sum_3_seq").between(4, 9))
    .select(
        "id",
        "commissioned_on_month_tariff_group_sum_installed",
        "next3_installed_kw_sum_3",
        "prev_installed_kw",
        "last_known_installed_kw_sum_4",
        "next_prev_installed_kw",
        "next3_installed_kw_sum_3_seq",
        "commissioned_on_month_tariff_group_sum_installed_3",
        "commissioned_on_month_tariff_group_sum_installed_6",
        (F.col("next3_installed_kw_sum_3") + F.col("prev_installed_kw")).alias("next3_installed_kw_sum_3_plus_prev_installed_kw"),
        F.least(F.col("next_prev_installed_kw"), F.col("prev_installed_kw")).alias("least_next_prev_installed_kw_prev_installed_kw"),
        F.least(F.col("next_prev_installed_kw"), F.col("prev_installed_kw")).alias("least_next_prev_installed_kw_prev_installed_kw_2"),
    )
)

result = (
    annotated
    .select(
        "id",
        "commissioned_on_month_tariff_group_sum_installed",
        "next3_installed_kw_sum_3",
        "last_known_installed_kw_sum_4",
        "next3_installed_kw_sum_3_seq",
        "commissioned_on_month_tariff_group_sum_installed_3",
        "commissioned_on_month_tariff_group_sum_installed_6",
        "next3_installed_kw_sum_3_plus_prev_installed_kw",
        "least_next_prev_installed_kw_prev_installed_kw_2",
        (F.floor(F.col("next_prev_installed_kw") / 100) * 100).alias("next_prev_installed_kw_bucket"),
        F.round(F.col("prev_installed_kw") / F.nullif(F.col("next3_installed_kw_sum_3"), F.lit(0)), 3).alias("prev_installed_kw_per_next3_installed_kw_sum_3"),
    )
    .orderBy(
        F.col("next3_installed_kw_sum_3_plus_prev_installed_kw").desc(),
        "commissioned_on_month_tariff_group_sum_installed_3",
    )
)
