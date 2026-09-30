from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

annotated = (
    prepared_tracks
    .select(
        "*",
        F.coalesce(F.col("days_since_release_date"), F.lit(0)).alias("days_since_release_date_filled"),
    )
)

labeled = (
    annotated
    .filter(F.length(F.trim(F.col("title_clean"))) >= 4)
    .withColumn("greatest_bpm_days_since_release_date", F.greatest(F.col("bpm"), F.col("days_since_release_date")))
)

enriched = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.days_since_release_date",
        "s.release_date_quarter",
        "s.days_since_release_date_filled",
        "s.greatest_bpm_days_since_release_date",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    enriched
    .cube("artist_country", "release_date_quarter")
    .agg(
        F.grouping("artist_country").alias("is_total_artist_country"),
        F.round(F.sum("greatest_bpm_days_since_release_date") / F.nullif(F.sum("days_since_release_date_filled"), F.lit(0)), 3).alias("greatest_bpm_days_since_release_date_to_days_sin"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(
        F.col("artist_country").asc_nulls_last(),
        F.col("release_date_quarter").asc_nulls_last(),
    )
)
