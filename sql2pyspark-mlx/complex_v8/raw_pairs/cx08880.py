from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_played_at = Window.orderBy("days_since_played_at", "id")
w_listener_id_device = Window.partitionBy("listener_id", "device")

recent_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

ranked = (
    recent_plays
    .select(
        "*",
        F.row_number().over(w_all_by_days_since_played_at).alias("seq_no"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_listener_id_device), 2).alias("days_since_played_at_vs_listener_id_device_avg"),
        F.size(F.collect_set("track_id").over(w_listener_id_device)).alias("distinct_track_id_in_listener_id_device"),
        F.sum("ms_played").over(w_listener_id_device).alias("listener_id_device_sum_ms_played"),
        F.lead("ms_played").over(w_all_by_days_since_played_at).alias("next_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played_2"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        "p.total_play_ms_played_2",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

result = (
    with_stats
    .cube("device", "listener_id")
    .agg(
        F.round(F.avg("days_since_played_at"), 2).alias("avg_days_since_played_at"),
    )
    .select(
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "listener_id",
        "avg_days_since_played_at",
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("listener_id").asc_nulls_last())
)
