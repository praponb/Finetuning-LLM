from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%ld%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        F.col("a.genre").alias("artist_genre"),
    )
)

grouped = (
    with_details
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("artist_genre") == "Jazz", 1)).alias("n_artist_genre_jazz"),
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_not_is_explicit"),
        F.count("*").alias("row_count"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
    )
)

result = (
    grouped
    .filter(F.col("distinct_artist_genres").between(1, 8))
    .select(
        "artist_id",
        "n_artist_genre_jazz",
        "n_not_is_explicit",
        "distinct_artist_genres",
        F.when(F.col("distinct_artist_genres") >= 6, "HIGH").when(F.col("distinct_artist_genres") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_artist_genres_tier"),
    )
    .orderBy(
        F.col("distinct_artist_genres_tier").asc_nulls_last(),
        F.col("artist_id").desc(),
    )
)
