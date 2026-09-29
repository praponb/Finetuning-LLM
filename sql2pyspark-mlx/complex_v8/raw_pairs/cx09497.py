from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 104) | (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.when(F.col("duration_sec") >= 357, "A").when(F.col("duration_sec") >= 213, "B").otherwise("C").alias("duration_sec_tier"),
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
    )
)

enriched_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.duration_sec_tier",
        "s.release_date_plus_30d",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    enriched_artist
    .rollup("artist_is_verified", "is_explicit", "artist_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("duration_sec_tier").alias("distinct_duration_sec_tiers"),
        F.count(F.when(F.lower(F.col("title")).like("%ha%"), 1)).alias("n_lower_title_like_ha"),
    )
    .orderBy(
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
