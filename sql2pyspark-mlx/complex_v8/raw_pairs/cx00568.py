from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
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
        F.size(F.col("tags")).alias("tags_count"),
    )
)

ticket_side = (
    valid_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) & (F.col("closed_at") >= F.lit("2025-01-16 00:00:00").cast("timestamp")))
    .select(
        "priority",
        F.col("channel").alias("category"),
        "opened_at",
        F.lit("ticket").alias("origin"),
    )
)

event_rows = (
    df_ticket_events
    .select(
        "actor",
        "event_type",
        "event_at",
        F.lit("event").alias("origin"),
    )
)

event_side = (
    df_ticket_events
    .select(
        "event_type",
        "actor",
        "event_at",
        F.lit("event").alias("origin"),
    )
)

result = (
    ticket_side
    .union(event_rows)
    .union(event_side)
)
