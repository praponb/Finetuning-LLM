from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 18)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
    )
)

enriched_agent = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        "agent_hired_on",
        "agent_team",
        F.date_trunc("week", F.col("agent_hired_on")).alias("agent_hired_on_week"),
    )
    .orderBy(F.col("agent_hired_on_week").desc())
)
