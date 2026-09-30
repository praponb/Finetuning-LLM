from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter((F.col("device") == "desktop") & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

listener_id_stats = (
    filtered
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

outliers = (
    filtered.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

result = (
    outliers
    .select("id", "ms_played", "ms_played_filled")
    .unpivot(
        ids=["id"],
        values=["ms_played", "ms_played_filled"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
