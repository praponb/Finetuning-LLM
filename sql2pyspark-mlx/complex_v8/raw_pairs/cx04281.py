from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("is_explicit")) | (F.col("bpm") > 96))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_children = (
    valid.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    with_children
    .groupingSets(
        [["play_source", "is_explicit", "artist_id"], ["play_source"]],
        "play_source", "is_explicit", "artist_id",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("play_source") != "radio", 1).otherwise(0)).alias("n_play_source_radio"),
    )
)
