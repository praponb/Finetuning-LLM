from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_installed_kw_moving_3 = Window.orderBy("installed_kw", "id").rowsBetween(-3, Window.currentRow)
w_zone_tariff = Window.partitionBy("zone", "tariff")
w_commissioned_on_month_tariff = Window.partitionBy(F.month("commissioned_on"), "tariff")
w_all_by_installed_kw = Window.orderBy("installed_kw", "id")
w_tariff_zone_by_commissioned = Window.partitionBy("tariff", "zone").orderBy(F.col("commissioned_on").desc())
w_zone_by_commissioned_on_ties = Window.partitionBy("zone").orderBy("commissioned_on")
w_tariff_zone_by_commissioned_2 = Window.partitionBy("tariff", "zone").orderBy(F.col("commissioned_on").desc(), "id")
w_distinct_zone_in_commissione = Window.partitionBy("distinct_zone_in_commissioned_on_month_tariff", F.month("commissioned_on"))
w_all_by_commissioned_on_cume = Window.orderBy(F.col("commissioned_on_cume_dist_2").desc())
w_all_by_commissioned_on_cume_2 = Window.orderBy(F.col("commissioned_on_cume_dist_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("M%"))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

with_trends = (
    base
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.round(F.avg("installed_kw").over(w_all_by_installed_kw_moving_3), 2).alias("installed_kw_avg_4"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_zone_tariff), 2).alias("installed_kw_vs_zone_tariff_avg"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_commissioned_on_month_tariff), 2).alias("installed_kw_vs_commissioned_on_month_tariff_avg"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_all_by_installed_kw)).alias("days_since_prev_commissioned_on"),
        F.size(F.collect_set("zone").over(w_commissioned_on_month_tariff)).alias("distinct_zone_in_commissioned_on_month_tariff"),
        F.round(F.cume_dist().over(w_tariff_zone_by_commissioned), 3).alias("commissioned_on_cume_dist"),
        F.round(F.cume_dist().over(w_zone_by_commissioned_on_ties), 3).alias("commissioned_on_cume_dist_2"),
        F.lag("commissioned_on").over(w_tariff_zone_by_commissioned_2).alias("prev_commissioned_on"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.round(F.col("commissioned_on_cume_dist") / F.nullif(F.sum("commissioned_on_cume_dist").over(w_distinct_zone_in_commissione), F.lit(0)), 4).alias("commissioned_on_cume_dist_share_of_distinct_zone"),
        F.round(F.percent_rank().over(w_all_by_commissioned_on_cume), 3).alias("commissioned_on_cume_dist_2_percent_rank"),
        F.sum("commissioned_on_cume_dist_2").over(w_distinct_zone_in_commissione).alias("distinct_zone_in_commissioned_on_month_tariff_co"),
        F.last("commissioned_on_cume_dist", ignorenulls=True).over(w_all_by_commissioned_on_cume_2).alias("last_known_commissioned_on_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "zone",
        "installed_kw",
        "installed_kw_avg_4",
        "installed_kw_vs_zone_tariff_avg",
        "days_since_prev_commissioned_on",
        "distinct_zone_in_commissioned_on_month_tariff",
        "commissioned_on_cume_dist_2",
        "prev_commissioned_on",
        "commissioned_on_cume_dist_2_percent_rank",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
    )
    .orderBy("installed_kw")
)
