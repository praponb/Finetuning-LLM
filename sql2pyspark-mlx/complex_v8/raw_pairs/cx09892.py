from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
    )
)

with_parent_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

enriched_agent = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.agent_name",
        "s.agent_team",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    enriched_agent
    .groupingSets(
        [["agent_id", "agent_tier"], ["agent_id"], ["agent_tier"], []],
        "agent_id", "agent_tier",
    )
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
