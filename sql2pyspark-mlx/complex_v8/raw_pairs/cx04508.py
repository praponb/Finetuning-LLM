from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device") != "car", 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") > 98052), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.duration_sec_filled",
        "s.duration_sec_plus_bpm",
        "s.play_device_count",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    exploded_detail
    .groupBy("is_explicit", "release_date")
    .pivot("play_source", ["radio", "search"])
    .agg(F.avg("bpm"))
    .orderBy("is_explicit", "release_date")
)
