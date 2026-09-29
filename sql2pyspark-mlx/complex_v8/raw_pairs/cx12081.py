from pyspark.sql import functions as F

covered = (
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

enriched = (
    covered
    .filter(F.col("event_type").isin("resolved", "assigned", "escalated"))
    .select(
        "*",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "event_at_plus_2h",
        "event_at_label",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at_plus_2h")).alias("days_since_event_at_plus_2h"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at_plus_2h")).alias("days_since_event_at_plus_2h_2"),
    )
    .orderBy(F.col("priority").asc_nulls_last())
)
