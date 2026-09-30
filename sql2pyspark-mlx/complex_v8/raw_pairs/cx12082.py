from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(~F.col("event_type").isin("escalated", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

exploded_detail = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_id",
        "s.event_event_at",
        F.col("t.id").alias("event_id_2"),
        "t.event_at",
        F.col("t.event_type").alias("event_event_type_2"),
    )
)

with_parent_ticket = (
    exploded_detail.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel") != "phone")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_id",
        "s.event_event_at",
        "s.event_id_2",
        "s.event_at",
        "s.event_event_type_2",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

combined_details_event = (
    with_parent_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id_2"), "inner")
    .filter(F.col("t.ticket_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_id",
        "s.event_id_2",
        "s.event_at",
        "s.event_event_type_2",
        F.col("t.ticket_id").alias("event_ticket_id"),
    )
)

result = (
    combined_details_event
    .filter(F.col("event_id_2").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_id",
        "event_id_2",
        "event_at",
    )
    .orderBy(
        F.col("event_id_2").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        "id",
        "event_id",
    )
    .limit(5)
)
