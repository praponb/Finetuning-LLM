from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_year_tariff = Window.partitionBy(F.year("commissioned_on"), "tariff")
w_zone_tariff_by_commissioned = Window.partitionBy("zone", "tariff").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_tariff = Window.partitionBy("zone", "tariff")
w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").desc(), "id")
w_tariff_commissioned_on_month = Window.partitionBy("tariff", F.month("commissioned_on")).orderBy("commissioned_on", "id")
w_zone_by_days_since_commissio = Window.partitionBy("zone").orderBy(F.col("days_since_commissioned_on").asc_nulls_last(), "id")
w_zone = Window.partitionBy("zone")
w_commissioned_on_decile_commi = Window.partitionBy("commissioned_on_decile", "commissioned_on_rank").orderBy("prev_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_commission = Window.orderBy("days_since_commissioned_on_vs_commissioned_on_ye", "id")
w_all_by_commissioned_on_decil = Window.orderBy(F.col("commissioned_on_decile").desc_nulls_first(), "id")
w_all_by_commissioned_on_decil_2 = Window.orderBy(F.col("commissioned_on_decile").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_sites
    .filter((F.col("zone") == "Z3") & (F.col("tariff") == "TOU"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
    )
)

with_trends = (
    scoped
    .select(
        "*",
        F.round(F.col("days_since_commissioned_on") - F.avg("days_since_commissioned_on").over(w_commissioned_on_year_tariff), 2).alias("days_since_commissioned_on_vs_commissioned_on_ye"),
        F.round(F.avg("days_since_commissioned_on").over(w_commissioned_on_year_tariff), 2).alias("commissioned_on_year_tariff_avg_days_since_commi"),
        F.sum("days_since_commissioned_on").over(w_zone_tariff_by_commissioned).alias("running_days_since_commissioned_on"),
        F.rank().over(w_all_by_commissioned_on).alias("commissioned_on_rank"),
        F.ntile(10).over(w_tariff_commissioned_on_month).alias("commissioned_on_decile"),
        (F.col("days_since_commissioned_on") - F.lag("days_since_commissioned_on").over(w_zone_by_days_since_commissio)).alias("days_since_commissioned_on_change"),
        F.lead("days_since_commissioned_on").over(w_zone_by_days_since_commissio).alias("next_days_since_commissioned_on"),
        F.max("days_since_commissioned_on").over(w_commissioned_on_year_tariff).alias("commissioned_on_year_tariff_max_days_since_commi"),
        F.lag("commissioned_on").over(w_zone_by_days_since_commissio).alias("prev_commissioned_on"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        "days_since_commissioned_on_vs_commissioned_on_ye",
        "commissioned_on_rank",
        "days_since_commissioned_on_change",
        "commissioned_on_year_tariff_max_days_since_commi",
        F.size(F.collect_set("tariff").over(w_zone)).alias("distinct_tariff_in_zone"),
        F.round(F.col("days_since_commissioned_on") / F.nullif(F.sum("days_since_commissioned_on").over(w_zone), F.lit(0)), 4).alias("days_since_commissioned_on_share_of_zone"),
        F.first("prev_commissioned_on", ignorenulls=True).over(w_commissioned_on_decile_commi).alias("first_known_prev_commissioned_on"),
        F.round(F.percent_rank().over(w_all_by_days_since_commission), 3).alias("days_since_commissioned_on_vs_commissioned_on_ye_2"),
        F.round(F.percent_rank().over(w_all_by_commissioned_on_decil), 3).alias("commissioned_on_decile_percent_rank"),
        F.count(F.lit(1)).over(w_commissioned_on_decile_commi).alias("running_count"),
        F.first("commissioned_on").over(w_all_by_commissioned_on_decil_2).alias("first_commissioned_on"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "days_since_commissioned_on_vs_commissioned_on_ye",
        "commissioned_on_rank",
        "days_since_commissioned_on_change",
        "commissioned_on_year_tariff_max_days_since_commi",
        "distinct_tariff_in_zone",
        "days_since_commissioned_on_share_of_zone",
        "first_known_prev_commissioned_on",
        "days_since_commissioned_on_vs_commissioned_on_ye_2",
        "commissioned_on_decile_percent_rank",
        "running_count",
        "first_commissioned_on",
        F.datediff(F.col("commissioned_on"), F.col("first_commissioned_on")).alias("days_first_commissioned_on_to_commissioned_on"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_commissioned_on")).alias("days_since_first_commissioned_on"),
    )
)
