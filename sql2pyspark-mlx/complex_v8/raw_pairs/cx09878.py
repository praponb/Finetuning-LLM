from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("is_explicit")) & (~F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

line_level_plays = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        "play_id",
        "play_listener_id",
        "play_played_at",
        "play_device",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
    .orderBy(
        F.col("play_played_at").desc(),
        F.col("days_since_release_date").desc_nulls_first(),
        "id",
        "play_id",
    )
    .limit(10)
)
