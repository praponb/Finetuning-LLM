from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((~F.col("priority").isin("P4", "P2")) | (F.col("csat").between(1, 4)))
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
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

combined_details = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_plus_csat",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

detail_rows_events = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

with_details = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.event_type").isin("replied", "escalated"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.agent_team",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        "s.event_type",
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    with_details
    .groupBy("event_event_type")
    .pivot("priority", ["P3", "P2"])
    .agg(F.count("csat"))
    .orderBy("event_event_type")
)
