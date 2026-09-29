from pyspark.sql import functions as F

base = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_details_track = (
    with_stats.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("t.artist_id") <= 12)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source_prefix",
        "s.play_count",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    with_details_track
    .filter(F.col("play_count").between(1, 8))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source_prefix",
        "play_count",
        "track_artist_id",
        "track_is_explicit",
        "listener_country",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.greatest(F.col("play_count"), F.col("ms_played")).alias("greatest_play_count_ms_played"),
    )
    .orderBy("ms_played")
)
