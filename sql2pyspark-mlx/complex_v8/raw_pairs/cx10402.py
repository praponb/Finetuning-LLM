from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-11-29").cast("date"))) & (F.col("duration_sec") >= 165))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
    )
)

filtered = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Jazz"), "left_semi")
)

line_level_plays = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.bpm_plus_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "play_id",
        "play_played_at",
        "play_is_skipped",
        "play_device",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.greatest(F.col("duration_sec"), F.col("bpm_plus_duration_sec")).alias("greatest_duration_sec_bpm_plus_duration_sec"),
    )
    .orderBy(F.col("title").desc(), "id", "play_id")
    .limit(20)
)
