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
    joined_ranges
    .crossJoin(joined_ranges.agg(F.max("hours_since_open").alias("overall_max_hours_since_open")))
    .filter(F.col("hours_since_open") < F.col("overall_max_hours_since_open"))
    .drop("overall_max_hours_since_open")
)

result = (
    filtered
    .select("hours_since_open", "event_type", "ticket_id")
    .distinct()
    .orderBy(F.col("event_type").desc(), "hours_since_open")
)
