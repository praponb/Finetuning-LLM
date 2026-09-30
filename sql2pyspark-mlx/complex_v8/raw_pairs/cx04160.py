from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_listener_id_b = Window.partitionBy("prior_rows_12h", "listener_id").orderBy(F.col("avg_prior_rows_12h").asc_nulls_last(), "played_at_month", "listener_id", "prior_rows_12h")
w_all_by_median_prior_rows_12h = Window.orderBy("median_prior_rows_12h", "played_at_month", "listener_id", "prior_rows_12h").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_median_prior_rows_12h_2 = Window.orderBy("median_prior_rows_12h", "played_at_month", "listener_id", "prior_rows_12h")

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

grouped = (
    covered
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "listener_id",
        "prior_rows_12h",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.percentile_approx("prior_rows_12h", 0.5).alias("median_prior_rows_12h"),
        F.round(F.avg("prior_rows_12h"), 2).alias("avg_prior_rows_12h"),
        F.avg("prior_ms_played_12h").alias("avg_prior_ms_played_12h_check"),
    )
    .filter(F.col("avg_prior_ms_played_12h_check") > 200000)
    .drop("avg_prior_ms_played_12h_check")
)

with_windows = (
    grouped
    .select(
        "*",
        F.ntile(10).over(w_prior_rows_12h_listener_id_b).alias("avg_prior_rows_12h_decile"),
        F.nth_value("distinct_listener_ids", 2).over(w_all_by_median_prior_rows_12h).alias("second_distinct_listener_ids"),
        F.lag("median_prior_rows_12h").over(w_all_by_median_prior_rows_12h_2).alias("prev_median_prior_rows_12h"),
    )
)

result = (
    with_windows
    .filter(F.to_date(F.col("played_at_month")) < F.lit("2025-02-09").cast("date"))
    .select(
        "played_at_month",
        "prior_rows_12h",
        "median_prior_rows_12h",
        "second_distinct_listener_ids",
        "prev_median_prior_rows_12h",
        F.coalesce(F.col("median_prior_rows_12h"), F.lit(0)).alias("median_prior_rows_12h_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at_month")).alias("days_since_played_at_month"),
    )
    .orderBy("played_at_month", "prior_rows_12h")
)
