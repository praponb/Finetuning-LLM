from pyspark.sql import functions as F

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

per_group = (
    in_window
    .groupBy(
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
        "listener_id",
    )
    .agg(
        F.sum("prior_ms_played_12h").alias("total_prior_ms_played_12h"),
        F.count("*").alias("row_count"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("prior_rows_12h"), 2).alias("avg_prior_rows_12h"),
    )
)

result = (
    per_group
    .select(
        "played_at_ym",
        "total_prior_ms_played_12h",
        "row_count",
        "distinct_listener_ids",
        "distinct_ids",
        "avg_prior_rows_12h",
    )
)
