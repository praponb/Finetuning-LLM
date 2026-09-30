from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("track_id") >= 5))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

qualified = (
    prepared
    .crossJoin(prepared.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

labeled = (
    qualified
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

result = (
    without_matches
    .cube("track_id", "is_skipped_label")
    .agg(
        F.sum(F.when(F.col("source") == "search", 1).otherwise(0)).alias("n_source_search"),
    )
    .select(
        "track_id",
        F.coalesce(F.col("is_skipped_label"), F.lit("ALL")).alias("is_skipped_label"),
        "n_source_search",
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("is_skipped_label").asc_nulls_last(),
    )
)
