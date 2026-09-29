from pyspark.sql import functions as F

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.ms_played").alias("prior_ms_played_1d"),
    )
)

annotated = (
    in_window
    .filter(F.col("prior_rows_1d") >= 2)
    .withColumn("prior_ms_played_1d_plus_prior_rows_1d", F.col("prior_ms_played_1d") + F.col("prior_rows_1d"))
)

result = (
    annotated
    .groupingSets(
        [["listener_id", "prior_rows_1d"], ["listener_id"], ["prior_rows_1d"], []],
        "listener_id", "prior_rows_1d",
    )
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.min("prior_rows_1d").alias("min_prior_rows_1d"),
        F.round(F.avg("prior_ms_played_1d_plus_prior_rows_1d"), 2).alias("avg_prior_ms_played_1d_plus_prior_rows_1d"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
