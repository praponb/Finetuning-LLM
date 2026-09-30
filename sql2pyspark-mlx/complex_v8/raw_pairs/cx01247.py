from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 402)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
    )
)

screened = (
    recent_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

with_details_artist = (
    screened.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    with_details_artist
    .rollup("artist_genre", "artist_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("artist_debut_year").alias("min_artist_debut_year"),
    )
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
