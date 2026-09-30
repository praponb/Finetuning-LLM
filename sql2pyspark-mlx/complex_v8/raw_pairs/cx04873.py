from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

enriched_agent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched_agent
    .groupBy("agent_id", "agent_team")
    .pivot("channel", ["web", "phone", "chat", "email"])
    .agg(F.min("requester_id"))
)
