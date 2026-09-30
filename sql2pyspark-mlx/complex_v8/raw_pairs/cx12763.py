from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(F.col("track_id") >= 25)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

with_parent = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        "s.played_at_dayofweek",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
    )
)

with_parent_track = (
    with_parent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        "s.played_at_dayofweek",
        "s.track_duration_sec",
        "s.track_moods",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit_2"),
    )
)

result = (
    with_parent_track
    .groupBy("track_release_date")
    .pivot("source", ["radio", "album"])
    .agg(F.avg("played_at_dayofweek"))
)
