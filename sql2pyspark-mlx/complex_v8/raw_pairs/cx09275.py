from pyspark.sql import functions as F

in_window = (
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

listener_id_stats = (
    in_window
    .groupBy("listener_id")
    .agg(F.percentile_approx("prior_ms_played_3d", 0.5).alias("median_prior_ms_played_3d"))
)

above_average = (
    in_window.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.prior_ms_played_3d") >= F.col("g.median_prior_ms_played_3d"))
    .select(
        "s.*",
        "g.median_prior_ms_played_3d",
        F.round(F.col("s.prior_ms_played_3d") - F.col("g.median_prior_ms_played_3d"), 2).alias("prior_ms_played_3d_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_ms_played_3d",
        "median_prior_ms_played_3d",
        "prior_ms_played_3d_minus_median",
        F.when(F.col("prior_rows_3d") >= 6, "large").when(F.col("prior_rows_3d") >= 2, "medium").otherwise("small").alias("prior_rows_3d_tier"),
        (F.floor(F.col("prior_ms_played_3d") / 1000000) * 1000000).alias("prior_ms_played_3d_bucket"),
    )
    .orderBy(F.col("played_at").asc(), F.col("prior_rows_3d_tier").asc_nulls_last())
)
