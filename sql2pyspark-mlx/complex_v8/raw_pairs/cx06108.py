from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

with_activity = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source") != "search"), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.min("played_at").alias("first_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.first_played_at",
        "p.total_play_ms_played",
    )
)

detail_rows_plays = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.played_at_date",
        "s.play_is_skipped_count",
        "s.first_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows_plays
    .cube("play_id", "track_id")
    .agg(
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.min("played_at_date").alias("first_played_at_date"),
    )
)
