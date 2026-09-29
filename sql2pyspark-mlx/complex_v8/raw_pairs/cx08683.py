from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("ms_played") <= 340114)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

combined_details_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source_ref",
        "s.source_prefix",
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

without_matches = (
    combined_details_track
    .crossJoin(combined_details_track.agg(F.max("ms_played").alias("overall_max_ms_played")))
    .filter(F.col("ms_played") < F.col("overall_max_ms_played"))
    .drop("overall_max_ms_played")
)

result = (
    without_matches
    .groupBy("track_id", "is_skipped")
    .pivot("device", ["phone", "speaker"])
    .agg(F.sum("ms_played"))
    .orderBy("track_id", "is_skipped")
)
