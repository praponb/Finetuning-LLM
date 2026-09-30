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

derived = (
    in_window
    .select(
        "*",
        F.coalesce(F.col("prior_rows_3d"), F.lit(0)).alias("prior_rows_3d_filled"),
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
    )
)

result = (
    derived
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_3d",
        "played_at_plus_2h",
    )
    .orderBy(F.col("listener_id").desc(), F.col("played_at").desc(), "id")
    .limit(5)
)
