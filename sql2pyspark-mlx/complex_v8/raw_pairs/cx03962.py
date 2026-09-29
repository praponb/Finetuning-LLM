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
        F.sum("b.requester_id").alias("prior_requester_id_3d"),
    )
)

with_details = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.agent_name").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_requester_id_3d",
        "su.agent_name",
    )
)

result = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "agent_name",
    )
    .orderBy("agent_name", F.col("agent_id").desc())
)
