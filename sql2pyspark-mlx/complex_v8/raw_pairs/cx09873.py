from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.lower(F.col("t.title")).like("% s%")) & (F.size(F.col("t.moods")) >= 1), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count(F.when(F.col("device") == "speaker", 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    screened.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("ms_played").between(92277, 251066))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source_clean",
        "min_play_ms_played",
        "avg_play_ms_played",
        "play_device_count",
    )
)
