from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

qualified = (
    valid_plays
    .crossJoin(valid_plays.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

result = (
    qualified
    .select("id", "days_since_played_at", "ms_played")
    .unpivot(
        ids=["id"],
        values=["days_since_played_at", "ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
