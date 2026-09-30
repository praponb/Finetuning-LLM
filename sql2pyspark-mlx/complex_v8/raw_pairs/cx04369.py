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

enriched = covered.withColumn("prior_rows_3d_filled", F.coalesce(F.col("prior_rows_3d"), F.lit(0)))

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_3d",
        "prior_rows_3d_filled",
        F.greatest(F.col("prior_ms_played_3d"), F.col("prior_rows_3d")).alias("greatest_prior_ms_played_3d_prior_rows_3d"),
        F.when(F.col("prior_rows_3d") >= 6, "large").when(F.col("prior_rows_3d") >= 3, "medium").otherwise("small").alias("prior_rows_3d_tier"),
    )
    .orderBy(F.col("played_at").asc(), F.col("prior_rows_3d").asc_nulls_last())
)
