from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_children = (
    scoped_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods_count",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

screened = (
    with_children.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")), "left_anti")
    .filter(F.col("play_source") == "playlist")
)

joined_artist = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.source").alias("play_source_2"),
    )
)

result = (
    joined_artist
    .select("play_source", "title")
    .distinct()
    .orderBy(F.col("title").asc_nulls_last())
)
