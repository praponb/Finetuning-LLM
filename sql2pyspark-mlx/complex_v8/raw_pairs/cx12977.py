from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 213).alias("duration_sec_distance"),
    )
)

without_matches = (
    scoped
    .crossJoin(scoped.agg(F.min("bpm").alias("overall_min_bpm")))
    .filter(F.col("bpm") > F.col("overall_min_bpm"))
    .drop("overall_min_bpm")
)

detail_rows_plays = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.duration_sec_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    detail_rows_plays
    .filter(F.col("play_ms_played").between(49807, 239955))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "duration_sec_distance",
        "play_id",
        "play_ms_played",
        "play_listener_id",
        F.coalesce(F.col("play_ms_played"), F.lit(0)).alias("play_ms_played_filled"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("play_listener_id").desc())
)
