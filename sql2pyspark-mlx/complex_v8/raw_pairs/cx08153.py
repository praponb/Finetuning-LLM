from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-26 18:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
    )
)

combined_details_agent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on") >= F.lit("2024-07-09").cast("date"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

line_level = (
    combined_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    line_level
    .filter(~F.col("agent_team").isin("Tech", "Billing"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "agent_team",
        "event_id",
        "event_actor",
    )
    .orderBy(F.col("agent_team").desc_nulls_first())
)
