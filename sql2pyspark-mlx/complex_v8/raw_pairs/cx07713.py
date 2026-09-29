from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

annotated = (
    joined_ranges
    .filter(F.col("agent_id") >= 9)
    .select(
        "*",
        F.concat_ws(": ", F.col("priority"), F.col("event_type")).alias("priority_event_type_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.min("requester_id").alias("min_ticket_requester_id"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

profiled = (
    annotated.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_closed_at",
        "t.min_ticket_requester_id",
        "t.first_ticket_closed_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_type",
        "agent_id",
        "hours_since_open",
        "days_since_event_at",
        "hours_since_open_filled",
        "last_ticket_closed_at",
        "first_ticket_closed_at",
        F.round(F.col("hours_since_open") / F.nullif(F.col("min_ticket_requester_id"), F.lit(0)), 3).alias("hours_since_open_per_min_ticket_requester_id"),
        F.date_trunc("day", F.col("last_ticket_closed_at")).alias("last_ticket_closed_at_day"),
    )
    .orderBy("id")
)
