from pyspark.sql import functions as F

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

labeled = (
    filtered
    .filter(F.col("duration_sec") >= 165)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 155) & (F.col("t.is_explicit")), "left_semi")
)

rollup_base = (
    qualified
    .groupBy("artist_id")
    .agg(
        F.sum("bpm").alias("total_bpm"),
        F.max("bpm").alias("max_bpm"),
        F.percentile_approx("bpm_filled", 0.5).alias("median_bpm_filled"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count("*").alias("row_count"),
    )
)

result = (
    rollup_base
    .select(
        "artist_id",
        "row_count",
        "total_bpm",
        "distinct_artist_ids",
    )
    .unpivot(
        ids=["artist_id"],
        values=["row_count", "total_bpm", "distinct_artist_ids"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("artist_id", "metric")
)
