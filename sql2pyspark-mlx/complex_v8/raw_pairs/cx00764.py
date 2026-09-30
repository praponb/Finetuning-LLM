from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_month_by_valid_from = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy(F.col("valid_from").desc(), "id")
w_valid_to_month_by_valid_from_2 = Window.partitionBy(F.date_trunc("month", "valid_to")).orderBy(F.col("valid_from").desc(), "id").rowsBetween(-3, -1)
w_valid_from_to_date = Window.partitionBy(F.to_date("valid_from"))

recent_tariffs = (
    df_tariff_windows
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.date_trunc("day", F.col("valid_to")).alias("valid_to_day"),
    )
)

qualified = (
    recent_tariffs
    .crossJoin(recent_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

with_trends = (
    qualified
    .select(
        "id",
        "valid_from",
        "valid_to",
        "valid_to_day",
        F.lag("rate").over(w_valid_to_month_by_valid_from).alias("prev_rate"),
        F.max("rate").over(w_valid_to_month_by_valid_from_2).alias("rate_max_3_prior"),
        F.sum("rate").over(w_valid_from_to_date).alias("valid_from_to_date_sum_rate"),
    )
)

without_matches = (
    with_trends
    .crossJoin(with_trends.agg(F.min("rate_max_3_prior").alias("overall_min_rate_max_3_prior")))
    .filter(F.col("rate_max_3_prior") > F.col("overall_min_rate_max_3_prior"))
    .drop("overall_min_rate_max_3_prior")
)

result = (
    without_matches
    .select(
        "id",
        "valid_to",
        "prev_rate",
        "rate_max_3_prior",
        "valid_from_to_date_sum_rate",
    )
    .orderBy(F.col("rate_max_3_prior").desc_nulls_first())
)
