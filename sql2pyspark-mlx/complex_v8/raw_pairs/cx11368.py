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
    .filter(F.col("ticket_id") >= 55)
    .withColumn("priority_event_type_label", F.concat_ws("-", F.col("priority"), F.col("event_type")))
)

result = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "priority_event_type_label",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
    .orderBy(F.col("hours_since_open").asc_nulls_last())
)
