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
        F.sum("b.csat").alias("prior_csat_7d"),
    )
)

totals = (
    matched
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_opened_at"),
        F.max("prior_csat_7d").alias("max_prior_csat_7d"),
        F.round(F.sum("prior_csat_7d") / F.nullif(F.sum("prior_rows_7d"), F.lit(0)), 3).alias("prior_csat_7d_to_prior_rows_7d_ratio"),
    )
)

result = (
    totals
    .select("agent_id", "last_opened_at", "max_prior_csat_7d")
    .orderBy(F.col("agent_id").desc(), F.col("max_prior_csat_7d").asc_nulls_last())
)
