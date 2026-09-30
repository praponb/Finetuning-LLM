from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

with_activity = (
    covered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("F%")), "left_anti")
)

result = (
    with_activity
    .select("id", "prior_rows_12h", "prior_csat_12h")
    .unpivot(
        ids=["id"],
        values=["prior_rows_12h", "prior_csat_12h"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .orderBy("id", "metric")
)
