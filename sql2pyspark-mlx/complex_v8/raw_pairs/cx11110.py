from pyspark.sql import functions as F

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.requester_id").alias("prior_requester_id_7d"),
    )
)

labeled = (
    matched
    .select(
        "*",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.abs(F.col("prior_rows_7d") - 3).alias("prior_rows_7d_distance"),
        F.coalesce(F.col("prior_rows_7d"), F.lit(0)).alias("prior_rows_7d_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_7d",
        "prior_requester_id_7d",
        "opened_at_date",
        "prior_rows_7d_filled",
        F.round(F.col("prior_requester_id_7d") / F.nullif(F.col("prior_rows_7d_filled"), F.lit(0)), 3).alias("prior_requester_id_7d_per_prior_rows_7d_filled"),
    )
    .orderBy(
        F.col("prior_rows_7d_filled").desc(),
        F.col("prior_requester_id_7d_per_prior_rows_7d_filled").desc(),
    )
)
