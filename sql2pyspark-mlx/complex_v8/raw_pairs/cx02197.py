from pyspark.sql import functions as F

joined_ranges = (
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

tagged = (
    joined_ranges
    .withColumn("days_since_played_at", F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")))
)

result = (
    tagged
    .rollup("listener_id", "prior_rows_12h")
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.round(F.stddev("days_since_played_at"), 2).alias("stddev_days_since_played_at"),
    )
)
