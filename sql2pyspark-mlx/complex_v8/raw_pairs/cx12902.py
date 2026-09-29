from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.col("bpm").between(85, 180)) & (F.col("release_date") >= F.lit("2024-09-26").cast("date")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.release_date_month",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_details_artist = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") <= 2020)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.release_date_month",
        "s.artist_genre",
        "s.artist_debut_year",
        "s.artist_is_verified",
        F.col("a.genre").alias("artist_genre_2"),
        F.col("a.debut_year").alias("artist_debut_year_2"),
    )
)

result = (
    with_details_artist
    .rollup("artist_genre_2", "artist_is_verified", "artist_genre")
    .agg(
        F.grouping("artist_genre_2").alias("is_total_artist_genre_2"),
        F.max("release_date").alias("last_release_date"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
    )
    .orderBy(
        F.col("artist_genre_2").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
    )
)
