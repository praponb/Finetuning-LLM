from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

rollup_base = (
    valid_tracks
    .filter(F.col("artist_id").isNotNull())
    .groupBy("artist_id")
    .agg(
        F.round(F.avg("days_since_release_date"), 2).alias("avg_days_since_release_date"),
        F.round(F.sum("bpm") / F.nullif(F.sum("days_since_release_date"), F.lit(0)), 3).alias("bpm_to_days_since_release_date_ratio"),
        F.count("*").alias("row_count"),
        F.sum("days_since_release_date").alias("total_days_since_release_date"),
    )
    .filter(F.col("row_count") >= 2)
)

combined_details_artist = (
    rollup_base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details_artist
    .select(
        "artist_id",
        "bpm_to_days_since_release_date_ratio",
        "avg_days_since_release_date",
    )
    .unpivot(
        ids=["artist_id"],
        values=["bpm_to_days_since_release_date_ratio", "avg_days_since_release_date"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("artist_id", "field")
)
