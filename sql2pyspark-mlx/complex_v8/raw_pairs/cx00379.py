from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("channel").isin("phone", "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

result = (
    with_parent
    .cube("agent_tier", "agent_id")
    .agg(
        F.grouping("agent_tier").alias("is_total_agent_tier"),
        F.min("requester_id").alias("min_requester_id"),
    )
)
