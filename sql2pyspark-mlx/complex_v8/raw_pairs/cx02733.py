from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_opened_at",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_stats
    .groupBy("ticket_id", "event_type")
    .pivot("priority", ["P2", "P3", "P1"])
    .agg(F.min("ticket_count"))
    .orderBy("ticket_id", "event_type")
)
