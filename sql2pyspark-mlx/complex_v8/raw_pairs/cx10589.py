from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") <= 9)
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

detail_rows = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.ticket_agent_id",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_id_2",
        "ticket_closed_at_2",
        "ticket_csat",
        "ticket_channel",
    )
)
