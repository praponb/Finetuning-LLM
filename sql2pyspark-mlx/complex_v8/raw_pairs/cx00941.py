from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.year(F.col("release_date")).alias("release_date_year"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

combined_details_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.artist_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.bpm_minus_duration_sec",
        "s.release_date_year",
        "s.title_ref",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

enriched = (
    combined_details_artist
    .filter(F.col("bpm_minus_duration_sec") > 114)
    .select(
        "*",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")) & (F.col("t.release_date").isNotNull()), "left_anti")
)

result = (
    qualified
    .cube("artist_id", "release_date_year")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("release_date_year").asc_nulls_last(),
    )
)
