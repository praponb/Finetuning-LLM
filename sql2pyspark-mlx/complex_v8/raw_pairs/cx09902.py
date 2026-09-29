from pyspark.sql import functions as F

in_window = (
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
    .agg(F.min("opened_at").alias("first_ticket_opened_at"))
)

with_stats = (
    in_window.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_opened_at")
)

detail_rows_events = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    detail_rows_events
    .filter(F.col("event_event_at") < F.lit("2025-03-02 15:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "hours_since_open",
        "event_event_at",
    )
    .orderBy(F.col("priority").desc(), "id")
)
