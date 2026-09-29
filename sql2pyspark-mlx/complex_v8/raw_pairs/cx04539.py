from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_commissioned_on_month = Window.partitionBy("zone", "commissioned_on_month")
w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy("commissioned_on", "id")
w_zone_by_commissioned_on_runn = Window.partitionBy("zone").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_commissioned_on_m = Window.partitionBy("last_known_commissioned_on_month").orderBy("commissioned_on_month", "id")
w_zone_by_rows_in_zone_commiss = Window.partitionBy("zone").orderBy("rows_in_zone_commissioned_on_month", "id")
w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").desc(), "id")
w_all_by_commissioned_on_prior = Window.orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(-1, -1)
w_zone_next2_commissioned_on_m = Window.partitionBy("zone", "next2_commissioned_on_month")

filtered = (
    df_sites
    .filter(F.col("installed_kw") >= 141.6)
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.month(F.col("commissioned_on")).alias("commissioned_on_month"),
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
        F.substring(F.trim(F.col("zone")), 1, 2).alias("zone_prefix"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.col("commissioned_on_month") / F.nullif(F.sum("commissioned_on_month").over(w_zone_commissioned_on_month), F.lit(0)), 4).alias("commissioned_on_month_share_of_zone_commissioned"),
        (F.col("commissioned_on_month") - F.lag("commissioned_on_month").over(w_zone_by_commissioned_on)).alias("commissioned_on_month_change"),
        F.row_number().over(w_zone_by_commissioned_on).alias("commissioned_on_seq"),
        F.count(F.lit(1)).over(w_zone_commissioned_on_month).alias("rows_in_zone_commissioned_on_month"),
        F.last("commissioned_on_month", ignorenulls=True).over(w_zone_by_commissioned_on_runn).alias("last_known_commissioned_on_month"),
        F.lead("commissioned_on_month", 2).over(w_zone_by_commissioned_on).alias("next2_commissioned_on_month"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.lag("commissioned_on_seq").over(w_last_known_commissioned_on_m).alias("prev_commissioned_on_seq"),
        F.lead("commissioned_on_month_change").over(w_zone_by_rows_in_zone_commiss).alias("next_commissioned_on_month_change"),
        F.rank().over(w_all_by_commissioned_on).alias("commissioned_on_rank"),
        F.row_number().over(w_last_known_commissioned_on_m).alias("rn"),
        F.rank().over(w_zone_by_rows_in_zone_commiss).alias("rows_in_zone_commissioned_on_month_rank"),
        F.round(F.avg("commissioned_on_month_change").over(w_all_by_commissioned_on_prior), 2).alias("commissioned_on_month_change_avg_1_prior"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        "tariff_clean",
        "commissioned_on_month_share_of_zone_commissioned",
        "commissioned_on_month_change",
        "commissioned_on_seq",
        "rows_in_zone_commissioned_on_month",
        "last_known_commissioned_on_month",
        "next2_commissioned_on_month",
        "prev_commissioned_on_seq",
        "rows_in_zone_commissioned_on_month_rank",
        "commissioned_on_month_change_avg_1_prior",
        F.max("commissioned_on_month").over(w_zone_next2_commissioned_on_m).alias("zone_next2_commissioned_on_month_max_commissione"),
        F.round(F.col("commissioned_on_month_change") / F.nullif(F.sum("commissioned_on_month_change").over(w_zone_next2_commissioned_on_m), F.lit(0)), 4).alias("commissioned_on_month_change_share_of_zone_next2"),
        F.round(F.col("commissioned_on_month_change") - F.avg("commissioned_on_month_change").over(w_zone_next2_commissioned_on_m), 2).alias("commissioned_on_month_change_vs_zone_next2_commi"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["commissioned_on_seq", "rows_in_zone_commissioned_on_month_rank"], ["commissioned_on_seq"], []],
        "commissioned_on_seq", "rows_in_zone_commissioned_on_month_rank",
    )
    .agg(
        F.grouping("commissioned_on_seq").alias("is_total_commissioned_on_seq"),
        F.percentile_approx("prev_commissioned_on_seq", 0.5).alias("median_prev_commissioned_on_seq"),
    )
    .orderBy(
        F.col("commissioned_on_seq").asc_nulls_last(),
        F.col("rows_in_zone_commissioned_on_month_rank").asc_nulls_last(),
    )
)
