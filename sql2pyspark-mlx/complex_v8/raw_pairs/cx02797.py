from pyspark.sql import functions as F

recent = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

totals = (
    recent
    .groupBy("source", "is_skipped", "listener_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("device") == "car", 1)) / F.count("*"), 1).alias("pct_device_car"),
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_not_is_skipped"),
    )
)

annotated = (
    totals
    .select(
        "*",
        F.abs(F.col("pct_device_car") - 50.0).alias("pct_device_car_distance"),
        F.abs(F.col("n_rows") - 8).alias("n_rows_distance"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

screened = (
    annotated
    .crossJoin(annotated.agg(F.min("n_rows_distance").alias("overall_min_n_rows_distance")))
    .filter(F.col("n_rows_distance") > F.col("overall_min_n_rows_distance"))
    .drop("overall_min_n_rows_distance")
)

result = (
    screened
    .select(
        "source",
        "is_skipped",
        F.col("n_rows").alias("m_n_rows"),
        F.col("n_rows_distance").alias("n_rows_distance_v"),
    )
    .unpivot(
        ids=["source", "is_skipped"],
        values=["m_n_rows", "n_rows_distance_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
