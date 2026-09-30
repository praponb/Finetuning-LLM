from pyspark.sql import functions as F

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

grouped = (
    covered
    .groupBy("prior_rows_3d")
    .agg(
        F.percentile_approx("prior_rows_3d", 0.5).alias("median_prior_rows_3d"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("played_at").alias("first_played_at"),
    )
)

result = (
    grouped
    .select(
        "prior_rows_3d",
        "distinct_ids",
        "distinct_listener_ids",
        "median_prior_rows_3d",
    )
    .unpivot(
        ids=["prior_rows_3d"],
        values=["distinct_ids", "distinct_listener_ids", "median_prior_rows_3d"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("prior_rows_3d", "metric")
)
