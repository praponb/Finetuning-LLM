from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "chill"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.year(F.col("release_date")).alias("release_date_year"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

per_group = (
    filtered
    .filter(F.col("duration_sec") < 365)
    .groupBy("title_len", "artist_id")
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(F.sum("release_date_year") / F.nullif(F.sum("days_since_release_date"), F.lit(0)), 3).alias("release_date_year_to_days_since_release_date_rat"),
        F.round(F.sum("days_since_release_date") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("days_since_release_date_to_duration_sec_ratio"),
    )
)

scored = (
    per_group
    .select(
        "title_len",
        "artist_id",
        "release_date_year_to_days_since_release_date_rat",
        "days_since_release_date_to_duration_sec_ratio",
        (F.col("title_len") - F.col("distinct_artist_ids")).alias("title_len_minus_distinct_artist_ids"),
        F.coalesce(F.col("distinct_artist_ids"), F.lit(0)).alias("distinct_artist_ids_filled"),
    )
)

line_level_tracks = (
    scored.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "inner")
    .select(
        "s.title_len",
        "s.artist_id",
        "s.release_date_year_to_days_since_release_date_rat",
        "s.days_since_release_date_to_duration_sec_ratio",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    line_level_tracks
    .select(
        "title_len",
        "artist_id",
        "release_date_year_to_days_since_release_date_rat",
        "days_since_release_date_to_duration_sec_ratio",
    )
    .unpivot(
        ids=["title_len", "artist_id"],
        values=["release_date_year_to_days_since_release_date_rat", "days_since_release_date_to_duration_sec_ratio"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("title_len", "artist_id", "measure")
)
