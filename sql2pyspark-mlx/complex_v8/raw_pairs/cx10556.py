from pyspark.sql import functions as F

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

qualified = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 11), "left_semi")
    .filter(F.col("source").isin("playlist", "radio"))
)

enriched_track = (
    qualified.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.duration_sec") >= 165)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    enriched_track
    .select("track_duration_sec", "track_id")
    .distinct()
)
