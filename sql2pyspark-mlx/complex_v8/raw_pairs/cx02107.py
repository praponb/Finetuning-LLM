from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.col("channel").isin("web", "email")) & (F.col("closed_at") >= F.lit("2025-01-16 00:00:00").cast("timestamp")))
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
    )
)

exploded_detail_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail_events
    .groupBy("closed_at", "event_id")
    .pivot("priority", ["P1", "P4"])
    .agg(F.sum("csat"))
    .orderBy("closed_at", "event_id")
)
