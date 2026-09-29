from pyspark.sql import functions as F

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

qualified = (
    joined_ranges.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") > 73625), "left_semi")
)

result = (
    qualified
    .cube("prior_rows_7d", "listener_id")
    .agg(
        F.percentile_approx("prior_rows_7d", 0.5).alias("median_prior_rows_7d"),
    )
    .orderBy(
        F.col("prior_rows_7d").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
