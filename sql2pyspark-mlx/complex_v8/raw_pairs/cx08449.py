from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("channel") == "phone")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

joined = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.tags",
        "s.days_since_closed_at",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    joined
    .filter(F.col("agent_tier") == "T3")
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "days_since_closed_at",
        "agent_tier",
        "agent_team",
    )
    .orderBy("opened_at", F.col("agent_team").desc())
)
