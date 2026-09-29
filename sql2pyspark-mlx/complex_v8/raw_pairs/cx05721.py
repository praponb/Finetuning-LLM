from pyspark.sql import functions as F

matched = (
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

stats = (
    matched
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
    )
)

result = (
    stats
    .select(
        "event_at_month",
        "stddev_hours_since_open",
        F.date_format(F.col("event_at_month"), "yyyy-MM").alias("event_at_month_label"),
    )
    .orderBy("stddev_hours_since_open")
)
