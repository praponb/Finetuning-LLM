from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (~F.col("event_type").isin("assigned", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.when(F.col("event_type").isin("assigned", "replied"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") > 3), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_hour",
        "s.event_type_group",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

with_counts = (
    with_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

enriched_ticket = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") <= 18)
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_group",
        "s.ticket_closed_at",
        "s.ticket_channel",
        "s.ticket_opened_at",
        "s.first_event_at",
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    enriched_ticket
    .filter(F.col("ticket_channel").isin("web", "email", "phone"))
    .select(
        "id",
        "actor",
        "event_type_group",
        "ticket_channel",
        "first_event_at",
        "ticket_closed_at_2",
        "ticket_requester_id",
        "ticket_priority",
        "agent_hired_on",
        F.abs(F.col("ticket_requester_id") - 7).alias("ticket_requester_id_distance"),
    )
    .orderBy(F.col("event_type_group").desc())
)
