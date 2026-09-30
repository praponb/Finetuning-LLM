from pyspark.sql import functions as F

matched = (
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

enriched = (
    matched
    .filter(F.col("prior_rows_3d") >= 1)
    .select(
        "*",
        F.greatest(F.col("prior_rows_3d"), F.col("prior_ms_played_3d")).alias("greatest_prior_rows_3d_prior_ms_played_3d"),
        F.when(F.col("prior_rows_3d") >= 6, "A").when(F.col("prior_rows_3d") >= 2, "B").otherwise("C").alias("prior_rows_3d_tier"),
    )
)

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "prior_rows_3d",
        "greatest_prior_rows_3d_prior_ms_played_3d",
        "prior_rows_3d_tier",
    )
    .orderBy(
        F.col("listener_id").desc(),
        "greatest_prior_rows_3d_prior_ms_played_3d",
    )
)
