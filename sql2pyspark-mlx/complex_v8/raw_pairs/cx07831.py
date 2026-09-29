from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "agent") | (F.col("event_type") == "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

joined_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.priority").isin("P2", "P1")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.agent_tier",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_children = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_priority",
        "s.ticket_closed_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children
    .filter(F.col("event_id") >= 3)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_priority",
        "event_id",
        "event_event_at",
        "event_actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.substring(F.trim(F.col("event_actor")), 1, 4).alias("event_actor_prefix"),
    )
    .orderBy(F.col("event_actor_prefix").desc(), "event_id")
)
