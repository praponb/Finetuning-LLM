from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
    )
)

joined_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    joined_artist.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("days_since_release_date") >= 315)
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "duration_sec_per_bpm",
        "duration_sec_plus_bpm",
        "artist_is_verified",
        "avg_play_ms_played",
        "play_count",
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)
