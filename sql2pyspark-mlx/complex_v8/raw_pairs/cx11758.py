from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("listener_id") >= 5))
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

enriched_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.duration_sec") < 309)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

result = (
    enriched_track
    .select("id", "track_duration_sec", "ms_played")
    .unpivot(
        ids=["id"],
        values=["track_duration_sec", "ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
