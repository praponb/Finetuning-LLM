from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_valid_from_month_by_v = Window.partitionBy("tariff", F.month("valid_from")).orderBy("valid_to_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_valid_from_month_by_v_2 = Window.partitionBy("tariff", F.month("valid_from")).orderBy("valid_to_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_from_month = Window.partitionBy(F.month("valid_from"))
w_all_by_rate_running = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rate_prior_1 = Window.orderBy(F.col("rate").desc(), "id").rowsBetween(-1, -1)
w_tariff_valid_to_to_date_by_r = Window.partitionBy("tariff", F.to_date("valid_to")).orderBy(F.col("rate").desc())

recent = (
    df_tariff_windows
    .filter(F.col("tariff").isin("EV", "TOU"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.date_trunc("month", F.col("valid_to")).alias("valid_to_month"),
        F.date_format(F.col("valid_from"), "yyyy-MM-dd HH:00").alias("valid_from_label"),
    )
)

qualified = (
    recent
    .crossJoin(recent.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

with_windows = (
    qualified
    .select(
        "*",
        F.min("rate").over(w_tariff_valid_from_month_by_v).alias("min_rate_so_far"),
        F.nth_value("valid_from", 2).over(w_tariff_valid_from_month_by_v_2).alias("second_valid_from"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_from_month), F.lit(0)), 4).alias("rate_share_of_valid_from_month"),
        F.first("rate", ignorenulls=True).over(w_all_by_rate_running).alias("first_known_rate"),
        F.sum("rate").over(w_all_by_rate_prior_1).alias("rate_sum_1_prior"),
        F.round(F.percent_rank().over(w_tariff_valid_to_to_date_by_r), 3).alias("rate_percent_rank"),
    )
)

enriched = (
    with_windows
    .filter(F.col("valid_from") >= F.lit("2025-01-24 14:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "valid_to_month",
        "min_rate_so_far",
        "second_valid_from",
        "first_known_rate",
        "rate_sum_1_prior",
        "rate_percent_rank",
        F.round(F.col("rate") * 0.9, 2).alias("rate_adj"),
    )
)

result = (
    enriched
    .select(
        "id",
        "valid_from",
        "rate",
        "valid_to_month",
        "min_rate_so_far",
        "rate_percent_rank",
        F.date_trunc("week", F.col("valid_to_month")).alias("valid_to_month_week"),
        F.substring(F.trim(F.col("tariff")), 1, 3).alias("tariff_prefix"),
    )
    .orderBy(F.col("tariff_prefix").asc_nulls_last())
)
