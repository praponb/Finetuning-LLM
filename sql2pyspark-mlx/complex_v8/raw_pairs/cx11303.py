from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_by_valid_fr = Window.partitionBy(F.month("valid_from")).orderBy("valid_from", "id")
w_valid_from_month_by_valid_fr_2 = Window.partitionBy(F.month("valid_from")).orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_month_by_valid_fr_3 = Window.partitionBy(F.month("valid_from")).orderBy("valid_from")
w_valid_from_month_by_valid_fr_4 = Window.partitionBy(F.month("valid_from")).orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff = Window.partitionBy("tariff")
w_valid_from_month_valid_to_ye = Window.partitionBy(F.month("valid_from"), F.year("valid_to"))
w_all_by_minutes_since_prev_va = Window.orderBy("minutes_since_prev_valid_from", "id").rowsBetween(-3, Window.currentRow)
w_all_by_valid_from_cume_dist = Window.orderBy(F.col("valid_from_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_minutes_since_prev_va_2 = Window.orderBy("minutes_since_prev_valid_from", "id")

scoped = df_tariff_windows.filter((F.col("tariff").isNotNull()) | (F.col("rate") > 3.0))

with_windows = (
    scoped
    .select(
        "*",
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_valid_from_month_by_valid_fr))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.last("valid_to").over(w_valid_from_month_by_valid_fr_2).alias("last_valid_to"),
        F.round(F.cume_dist().over(w_valid_from_month_by_valid_fr_3), 3).alias("valid_from_cume_dist"),
        F.last("rate", ignorenulls=True).over(w_valid_from_month_by_valid_fr_4).alias("last_known_rate"),
        F.round(F.col("rate") - F.avg("rate").over(w_tariff), 2).alias("rate_vs_tariff_avg"),
        F.round(F.percent_rank().over(w_valid_from_month_by_valid_fr_3), 3).alias("valid_from_percent_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "valid_from",
        "minutes_since_prev_valid_from",
        "last_known_rate",
        "valid_from_percent_rank",
        F.round(F.col("rate") - F.avg("rate").over(w_valid_from_month_valid_to_ye), 2).alias("rate_vs_valid_from_month_valid_to_year_avg"),
        F.round(F.avg("valid_from_percent_rank").over(w_all_by_minutes_since_prev_va), 2).alias("valid_from_percent_rank_avg_4"),
        F.last("valid_from").over(w_all_by_valid_from_cume_dist).alias("last_valid_from"),
        F.round(F.percent_rank().over(w_all_by_minutes_since_prev_va_2), 3).alias("minutes_since_prev_valid_from_percent_rank"),
        F.count(F.lit(1)).over(w_valid_from_month_valid_to_ye).alias("rows_in_valid_from_month_valid_to_year"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("valid_from_percent_rank_avg_4").alias("m_valid_from_percent_rank_avg_4"),
        F.col("minutes_since_prev_valid_from_percent_rank").alias("m_minutes_since_prev_valid_from_percent_rank"),
    )
    .unpivot(
        ids=["id"],
        values=["m_valid_from_percent_rank_avg_4", "m_minutes_since_prev_valid_from_percent_rank"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
