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

filtered = (
    covered.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date") >= F.lit("2024-08-02").cast("date")) & (F.array_contains(F.col("l.interests"), "kpop")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_3d",
        F.coalesce(F.col("prior_rows_3d"), F.lit(0)).alias("prior_rows_3d_filled"),
    )
    .orderBy("id")
)
