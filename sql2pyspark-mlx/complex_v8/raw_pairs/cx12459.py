from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
    )
)

detail_rows_events = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") != "created"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    detail_rows_events
    .groupBy("event_at")
    .pivot("event_type", ["escalated", "reopened", "resolved", "assigned"])
    .agg(F.avg("requester_id"))
)
