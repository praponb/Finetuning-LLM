from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date")) & (F.col("ticket_id") >= 51))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

joined_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_parent_ticket = (
    joined_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.ticket_requester_id",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

scored = (
    with_parent_ticket
    .filter(F.col("event_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.round(F.col("ticket_requester_id_2") / F.nullif(F.col("ticket_requester_id"), F.lit(0)), 3).alias("ticket_requester_id_2_per_ticket_requester_id"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.col("ticket_requester_id_2"), F.lit(0)), 3).alias("ticket_requester_id_per_ticket_requester_id_2"),
        F.greatest(F.col("ticket_requester_id_2"), F.col("ticket_requester_id")).alias("greatest_ticket_requester_id_2_ticket_requester_"),
    )
)

joined = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_priority",
        "s.ticket_opened_at",
        "s.ticket_requester_id_2_per_ticket_requester_id",
        "s.ticket_requester_id_per_ticket_requester_id_2",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.priority").alias("ticket_priority_2"),
        F.col("su.tier").alias("agent_tier"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_stats = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

result = (
    with_stats
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_priority",
        "ticket_opened_at",
        "ticket_requester_id_2_per_ticket_requester_id",
        "ticket_requester_id_per_ticket_requester_id_2",
        "ticket_opened_at_2",
        "ticket_priority_2",
        "agent_tier",
        "first_event_at_2",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_priority_2")), " ", "_")).alias("ticket_priority_2_slug"),
    )
    .orderBy(F.col("ticket_priority_2").desc(), "event_at")
)
