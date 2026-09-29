from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_played_at_month = Window.partitionBy("is_skipped", F.month("played_at"))
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_ms_played_seq_by_play = Window.partitionBy("device", "ms_played_seq").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_device_ms_played_seq_by_play_2 = Window.partitionBy("device", "ms_played_seq").orderBy(F.col("played_at").asc_nulls_last())

valid_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

metrics = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped_played_at_month), 2).alias("ms_played_vs_is_skipped_played_at_month_avg"),
        F.sum("ms_played").over(w_all_by_ms_played_running).alias("running_ms_played"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "ms_played_seq",
        "ms_played_vs_is_skipped_played_at_month_avg",
        F.round(F.percent_rank().over(w_device_ms_played_seq_by_play), 3).alias("played_at_percent_rank"),
        F.dense_rank().over(w_device_ms_played_seq_by_play_2).alias("played_at_dense_rank"),
        F.round(F.percent_rank().over(w_device_ms_played_seq_by_play_2), 3).alias("played_at_percent_rank_2"),
    )
)

joined = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.device",
        "s.ms_played_seq",
        "s.ms_played_vs_is_skipped_played_at_month_avg",
        "s.played_at_percent_rank",
        "s.played_at_percent_rank_2",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

profiled = (
    joined.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played", "p.avg_play_ms_played")
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played_2"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

profiled_2 = (
    profiled.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played_2",
        "p.min_play_ms_played",
    )
)

result = (
    profiled_2
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "ms_played_seq",
        "ms_played_vs_is_skipped_played_at_month_avg",
        "played_at_percent_rank",
        "played_at_percent_rank_2",
        "listener_interests",
        "listener_signup_date",
        "max_play_ms_played",
        "avg_play_ms_played",
        "play_count",
        "min_play_ms_played",
    )
    .orderBy("play_count")
)
