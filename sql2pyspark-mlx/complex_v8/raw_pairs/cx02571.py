from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) | (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.when(F.col("bpm") >= 142, "HIGH").when(F.col("bpm") >= 85, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.abs(F.col("bpm") - 96).alias("bpm_distance"),
    )
)

annotated = base_tracks.withColumn("moods_count", F.size(F.col("moods")))

combined_details = (
    annotated.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.title_clean",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["artist_country", "artist_id"], ["artist_country"], ["artist_id"], []],
        "artist_country", "artist_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.percentile_approx("duration_sec", 0.5).alias("median_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .select(
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "artist_id",
        "gid",
        "release_date_span_days",
        "median_duration_sec",
        "min_duration_sec",
    )
    .orderBy(
        F.col("artist_country").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
