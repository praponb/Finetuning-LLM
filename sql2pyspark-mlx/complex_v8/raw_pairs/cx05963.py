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

labeled = (
    covered
    .select(
        "*",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.concat_ws(": ", F.col("priority"), F.col("event_type")).alias("priority_event_type_label"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

result = (
    labeled
    .groupBy("event_at_month")
    .pivot("priority", ["P3", "P2", "P4", "P1"])
    .agg(F.min("hours_since_open_bucket"))
    .orderBy("event_at_month")
)
