from pyspark.sql import functions as F

matched = (
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

joined_listener = (
    matched.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    joined_listener
    .groupBy("prior_rows_12h")
    .pivot("listener_country", ["JP", "KR", "TH"])
    .agg(F.min("prior_ms_played_12h"))
)
