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

filtered = (
    joined_ranges.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.array_contains(F.col("t.tags"), "bug")) & (F.col("t.opened_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp")), "left_semi")
    .filter(F.hour(F.col("event_at")).between(8, 13))
)

with_parent_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    with_parent_ticket
    .filter(F.col("hours_since_open") <= 50.4)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "ticket_priority",
    )
    .orderBy(
        F.col("hours_since_open").asc_nulls_last(),
        F.col("id").desc_nulls_first(),
    )
)
