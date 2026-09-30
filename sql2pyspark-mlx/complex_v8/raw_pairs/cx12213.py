from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-07-01").cast("date")) & (F.col("bpm") < 155))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
    )
)

detail_rows = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["artist_id", "play_listener_id", "is_explicit"], ["artist_id"], []],
        "artist_id", "play_listener_id", "is_explicit",
    )
    .agg(
        F.sum(F.when(F.col("is_explicit"), F.col("play_ms_played"))).alias("play_ms_played_is_explicit_true"),
        F.round(F.stddev("play_ms_played"), 2).alias("stddev_play_ms_played"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("play_listener_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
