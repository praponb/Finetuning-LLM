from pyspark.sql import functions as F

covered = (
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

rollup_base = (
    covered
    .filter(F.col("prior_ms_played_1d") < 1700000)
    .groupBy(
        "listener_id",
        F.to_date("played_at").alias("played_at_day"),
        "prior_rows_1d",
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("cnt"),
        F.max("played_at").alias("last_played_at"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)

result = (
    rollup_base
    .select(
        "listener_id",
        "prior_rows_1d",
        "first_played_at",
        "distinct_listener_ids",
    )
)
