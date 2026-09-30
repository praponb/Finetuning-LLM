from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

joined_track = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.moods").alias("track_moods"),
    )
)

result = (
    joined_track
    .filter(F.col("listener_id") >= 2)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "track_artist_id",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy("device", "id")
)
