from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
    )
)

with_children_plays = (
    recent_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source_prefix",
        "s.ms_played_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    with_children_plays
    .groupBy("listener_id", "track_id")
    .pivot("play_device", ["phone", "desktop"])
    .agg(F.sum("ms_played_distance"))
)
