from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
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

derived = (
    matched
    .select(
        "id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
        F.abs(F.col("hours_since_open") - 25.2).alias("hours_since_open_distance"),
    )
)

result = (
    derived
    .filter(F.col("priority") == "P2")
    .select("id", "agent_id", "hours_since_open_filled")
)
