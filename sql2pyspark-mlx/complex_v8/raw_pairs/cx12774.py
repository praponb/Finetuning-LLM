from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
    )
)

joined_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details_agent = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"), "su.agent_name")
)

result = (
    with_details_agent
    .select(
        "id",
        "agent_id",
        "agent_tier",
        "agent_hired_on",
        "agent_name",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy("agent_hired_on", F.col("agent_name").desc_nulls_first())
)
