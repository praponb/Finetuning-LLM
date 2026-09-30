from pyspark.sql import functions as F

scoped = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
    )
)

enriched_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.duration_sec_distance",
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    enriched_artist
    .rollup("artist_genre", "is_explicit", "artist_id")
    .agg(
        F.sum(F.when(F.col("artist_genre").isin("Pop", "HipHop", "Jazz"), F.col("duration_sec_distance"))).alias("duration_sec_distance_artist_genre_in_pop_hiphop"),
    )
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
