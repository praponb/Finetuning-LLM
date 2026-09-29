from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-10-23").cast("date")) & (F.col("bpm") <= 156))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

summary = (
    base
    .groupBy(
        "artist_id",
        F.date_format("release_date_plus_30d", "yyyy-MM").alias("release_date_plus_30d_ym"),
        "is_explicit",
    )
    .agg(
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true_2"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.max("release_date_plus_30d").alias("last_release_date_plus_30d"),
        F.max("bpm").alias("max_bpm_check"),
    )
    .filter(F.col("max_bpm_check") > 89)
    .drop("max_bpm_check")
)

without_matches = (
    summary.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") <= 170) & (F.array_contains(F.col("t.moods"), "sad")), "left_anti")
)

enriched = (
    without_matches.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") > 1994)
    .select(
        "s.artist_id",
        "s.release_date_plus_30d_ym",
        "s.is_explicit",
        "s.n_is_explicit_true",
        "s.n_is_explicit_true_2",
        "s.distinct_artist_ids",
        "s.last_release_date_plus_30d",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

result = (
    enriched
    .select(
        "artist_id",
        "release_date_plus_30d_ym",
        "n_is_explicit_true",
        "artist_debut_year",
        "distinct_artist_ids",
    )
    .unpivot(
        ids=["artist_id", "release_date_plus_30d_ym"],
        values=["n_is_explicit_true", "artist_debut_year", "distinct_artist_ids"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
