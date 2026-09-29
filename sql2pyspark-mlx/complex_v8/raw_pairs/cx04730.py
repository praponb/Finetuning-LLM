from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("played_at") < F.lit("2025-03-08 08:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_details_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") <= 402), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.ms_played_bucket",
        "s.played_at_label",
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    with_details_track
    .cube("track_is_explicit", "is_skipped")
    .agg(F.max("played_at").alias("last_played_at"))
)
