from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_played_at_month_by_fir = Window.partitionBy(F.date_trunc("month", "first_played_at")).orderBy("first_played_at", "played_at_month")
w_first_played_at_month_played = Window.partitionBy(F.month("first_played_at"), F.year("played_at_month")).orderBy("last_played_at", "played_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_played_at_month_played_2 = Window.partitionBy(F.month("first_played_at"), F.year("played_at_month")).orderBy("last_played_at")
w_all_by_first_played_at = Window.orderBy(F.col("first_played_at").desc(), "played_at_month")
w_first_played_at_month_by_fir_2 = Window.partitionBy(F.date_trunc("month", "first_played_at")).orderBy("first_played_at", "played_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_played_at_month_played_3 = Window.partitionBy(F.month("first_played_at"), F.year("played_at_month")).orderBy("last_played_at", "played_at_month")
w_last_played_at_rank_last_kno = Window.partitionBy("last_played_at_rank", F.to_date("last_known_played_at_month")).orderBy("seq_no", "played_at_month")
w_second_played_at_month_month = Window.partitionBy(F.month("second_played_at_month"))
w_all_by_last_played_at_ties = Window.orderBy("last_played_at")
w_last_played_at_quartile_last = Window.partitionBy("last_played_at_quartile", F.year("last_known_played_at_month")).orderBy("last_played_at", "played_at_month")

filtered = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(6, 9))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.max_play_ms_played",
        "p.min_play_ms_played",
    )
)

summary = (
    profiled
    .filter(F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .groupBy(F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
        F.max("play_is_skipped_count").alias("max_play_is_skipped_count_check"),
    )
    .filter(F.col("max_play_is_skipped_count_check") > 1)
    .drop("max_play_is_skipped_count_check")
)

ranked = (
    summary
    .select(
        "*",
        F.row_number().over(w_first_played_at_month_by_fir).alias("seq_no"),
        F.last("played_at_month", ignorenulls=True).over(w_first_played_at_month_played).alias("last_known_played_at_month"),
        F.rank().over(w_first_played_at_month_played_2).alias("last_played_at_rank"),
        F.ntile(5).over(w_all_by_first_played_at).alias("first_played_at_quintile"),
        F.nth_value("played_at_month", 2).over(w_first_played_at_month_by_fir_2).alias("second_played_at_month"),
        F.ntile(4).over(w_first_played_at_month_played_3).alias("last_played_at_quartile"),
    )
)

screened = (
    ranked
    .crossJoin(ranked.agg(F.avg("last_played_at_rank").alias("overall_avg_last_played_at_rank")))
    .filter(F.col("last_played_at_rank") > F.col("overall_avg_last_played_at_rank"))
    .drop("overall_avg_last_played_at_rank")
)

with_windows = (
    screened
    .select(
        "*",
        F.ntile(4).over(w_last_played_at_rank_last_kno).alias("seq_no_quartile"),
        F.min("last_played_at_rank").over(w_second_played_at_month_month).alias("second_played_at_month_month_min_last_played_at_"),
        F.max("last_played_at_quartile").over(w_second_played_at_month_month).alias("second_played_at_month_month_max_last_played_at_"),
        F.dense_rank().over(w_all_by_last_played_at_ties).alias("last_played_at_dense_rank"),
        F.row_number().over(w_last_played_at_quartile_last).alias("last_played_at_seq"),
    )
)

result = (
    with_windows
    .select("played_at_month", "seq_no_quartile", "last_played_at_seq")
    .unpivot(
        ids=["played_at_month"],
        values=["seq_no_quartile", "last_played_at_seq"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
