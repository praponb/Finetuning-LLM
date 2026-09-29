from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_by_rate = Window.partitionBy(F.year("valid_to")).orderBy(F.col("rate").desc(), "id")
w_all_by_valid_from_full = Window.orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_valid_to_year_by_rate_runnin = Window.partitionBy(F.year("valid_to")).orderBy(F.col("rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_month = Window.partitionBy(F.month("valid_to"))
w_rate_rank = Window.partitionBy("rate_rank")
w_all_by_valid_from_ties = Window.orderBy(F.col("valid_from").desc())
w_valid_from_rank_by_valid_fro = Window.partitionBy("valid_from_rank").orderBy(F.col("valid_from").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_valid_from_running = Window.orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_from_rank_valid_from_p = Window.partitionBy("valid_from_rank", F.year("valid_from_plus_30m"))
w_all_by_valid_from = Window.orderBy("valid_from", "id")

cleaned = (
    df_tariff_windows
    .filter(F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.rank().over(w_valid_to_year_by_rate).alias("rate_rank"),
        F.nth_value("rate", 2).over(w_all_by_valid_from_full).alias("second_rate"),
        F.first("valid_from", ignorenulls=True).over(w_valid_to_year_by_rate_runnin).alias("first_known_valid_from"),
        F.max("rate").over(w_valid_to_month).alias("valid_to_month_max_rate"),
    )
)

with_activity = (
    ranked
    .crossJoin(ranked.agg(F.avg("second_rate").alias("overall_avg_second_rate")))
    .filter(F.col("second_rate") > F.col("overall_avg_second_rate"))
    .drop("overall_avg_second_rate")
)

metrics = (
    with_activity
    .select(
        "id",
        "valid_from",
        "rate",
        F.round(F.col("rate_rank") / F.nullif(F.sum("rate_rank").over(w_rate_rank), F.lit(0)), 4).alias("rate_rank_share_of_rate_rank"),
        F.round(F.col("second_rate") - F.avg("second_rate").over(w_rate_rank), 2).alias("second_rate_vs_rate_rank_avg"),
        F.rank().over(w_all_by_valid_from_ties).alias("valid_from_rank"),
    )
)

tagged = (
    metrics
    .filter(F.col("rate") > 3.0)
    .select(
        "*",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.date_trunc("year", F.col("valid_from")).alias("valid_from_year"),
        (F.col("valid_from") + F.expr("INTERVAL 30 MINUTES")).alias("valid_from_plus_30m"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.nth_value("second_rate_vs_rate_rank_avg", 2).over(w_valid_from_rank_by_valid_fro).alias("second_second_rate_vs_rate_rank_avg"),
        F.nth_value("valid_from_rank", 2).over(w_valid_from_rank_by_valid_fro).alias("second_valid_from_rank"),
        F.max("second_rate_vs_rate_rank_avg").over(w_all_by_valid_from_running).alias("max_second_rate_vs_rate_rank_avg_so_far"),
        F.round(F.cume_dist().over(w_all_by_valid_from), 3).alias("valid_from_cume_dist"),
        F.round(F.avg("rate").over(w_valid_from_rank_valid_from_p), 2).alias("valid_from_rank_valid_from_plus_30m_year_avg_rat"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "rate",
        "second_rate_vs_rate_rank_avg",
        "valid_from_rank",
        "rate_filled",
        "valid_from_year",
        "valid_from_plus_30m",
        "second_second_rate_vs_rate_rank_avg",
        "second_valid_from_rank",
        "valid_from_rank_valid_from_plus_30m_year_avg_rat",
    )
    .orderBy("second_second_rate_vs_rate_rank_avg")
)
