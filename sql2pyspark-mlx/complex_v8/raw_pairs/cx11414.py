from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_children_events = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at_day",
        "s.days_since_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

with_children = (
    with_children_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_actor",
        "s.event_event_at",
        F.col("t.id").alias("event_id_2"),
        "t.event_type",
        "t.event_at",
    )
)

joined_event = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id_2"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_actor",
        "s.event_id_2",
        "s.event_type",
        "s.event_at",
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

with_parent_ticket = (
    joined_event.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.event_ticket_id"), "inner")
    .filter(F.col("t.channel").isin("chat", "web"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_actor",
        "s.event_id_2",
        "s.event_type",
        "s.event_at",
        "s.event_actor_2",
        "s.event_ticket_id",
        "s.event_event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

filtered = (
    with_parent_ticket
    .crossJoin(with_parent_ticket.agg(F.min("ticket_requester_id").alias("overall_min_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_min_ticket_requester_id"))
    .drop("overall_min_ticket_requester_id")
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_id_2",
        "s.event_at",
        "s.event_actor_2",
        "s.event_ticket_id",
        "s.event_event_type",
        "s.ticket_channel",
        F.col("t.csat").alias("ticket_csat_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_id",
        "event_id_2",
        "event_at",
        "event_ticket_id",
        "event_event_type",
        "ticket_channel",
    )
    .orderBy(F.col("event_id").desc())
)
