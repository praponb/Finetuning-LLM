from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("C%")) & (F.col("duration_sec") > 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

detail_rows_plays = (
    scoped_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.duration_sec_filled",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

labeled = (
    detail_rows_plays
    .filter(F.col("artist_id") <= 8)
    .select(
        "*",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.col("duration_sec_filled") - F.col("bpm")).alias("duration_sec_filled_minus_bpm"),
    )
)

result = (
    labeled
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        "duration_sec_filled",
        "play_id",
        "play_ms_played",
        "play_is_skipped",
        "play_played_at",
        "title_len",
        "duration_sec_filled_minus_bpm",
    )
    .orderBy(F.col("title_len").desc_nulls_first())
)
