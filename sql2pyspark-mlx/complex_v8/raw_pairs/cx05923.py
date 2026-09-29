from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("duration_sec") < 409) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

with_parent_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.release_date_plus_7d",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_children_plays = (
    with_parent_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_children_plays
    .groupingSets(
        [["play_id", "artist_id"], ["play_id"], []],
        "play_id", "artist_id",
    )
    .agg(
        F.min("play_played_at").alias("first_play_played_at"),
        F.min("play_ms_played").alias("min_play_ms_played"),
        F.sum("play_ms_played").alias("total_play_ms_played"),
    )
    .orderBy(F.col("play_id").asc_nulls_last(), F.col("artist_id").asc_nulls_last())
)
