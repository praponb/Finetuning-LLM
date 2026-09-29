from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

with_details = (
    prepared_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        "s.source_clean",
        "s.played_at_hour",
        F.col("t.title").alias("track_title"),
        F.col("l.username").alias("listener_username"),
    )
)

line_level = (
    with_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        "s.played_at_hour",
        "s.track_title",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.sum("ms_played").alias("total_play_ms_played"))
)

with_stats = (
    line_level.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.total_play_ms_played")
)

result = (
    with_stats
    .groupingSets(
        [["play_id", "track_id", "play_source"], ["play_id"], []],
        "play_id", "track_id", "play_source",
    )
    .agg(
        F.round(F.sum("total_play_ms_played") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("total_play_ms_played_to_ms_played_ratio"),
        F.count("*").alias("n_rows"),
        F.max("ms_played").alias("max_ms_played"),
    )
    .select(
        "play_id",
        "track_id",
        F.coalesce(F.col("play_source"), F.lit("ALL")).alias("play_source"),
        "total_play_ms_played_to_ms_played_ratio",
        "n_rows",
        "max_ms_played",
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("play_source").asc_nulls_last(),
    )
)
