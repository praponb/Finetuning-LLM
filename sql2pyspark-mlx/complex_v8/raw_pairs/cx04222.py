from pyspark.sql import functions as F

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

annotated = (
    matched
    .select(
        "id",
        "agent_id",
        "prior_csat_3d",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
        F.when(F.col("prior_rows_3d") >= 6, "HIGH").when(F.col("prior_rows_3d") >= 3, "MEDIUM").otherwise("LOW").alias("prior_rows_3d_tier"),
    )
)

agent_id_stats = (
    annotated
    .groupBy("agent_id")
    .agg(F.avg("prior_csat_3d").alias("avg_prior_csat_3d"))
)

vs_group = (
    annotated.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_csat_3d") > F.col("g.avg_prior_csat_3d"))
    .select(
        "s.*",
        "g.avg_prior_csat_3d",
        F.round(F.col("s.prior_csat_3d") - F.col("g.avg_prior_csat_3d"), 2).alias("prior_csat_3d_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "agent_id",
        "prior_csat_3d",
        "opened_at_year",
        "prior_rows_3d_tier",
        "prior_csat_3d_minus_avg",
    )
    .orderBy(F.col("prior_rows_3d_tier").desc(), "id")
    .limit(5)
)
