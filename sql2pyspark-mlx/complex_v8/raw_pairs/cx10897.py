from pyspark.sql import functions as F

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

labeled = (
    with_stats
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "agent_id",
        "hours_since_open",
        "first_event_at",
        F.date_trunc("day", F.col("last_event_at")).alias("last_event_at_day"),
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("first_event_at"))) / 3600, 1).alias("hours_first_event_at_to_event_at"),
        F.abs(F.col("hours_since_open") - 50.4).alias("hours_since_open_distance"),
    )
)

result = (
    labeled
    .groupBy("ticket_id")
    .pivot("priority", ["P4", "P2", "P1"])
    .agg(F.sum("hours_first_event_at_to_event_at"))
    .orderBy("ticket_id")
)
