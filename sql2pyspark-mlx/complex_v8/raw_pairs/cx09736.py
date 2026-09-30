from pyspark.sql import functions as F

covered = (
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

with_activity = (
    covered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") > 3), "left_anti")
)

result = (
    with_activity
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "hours_since_open",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
    )
    .orderBy("id", F.col("hours_since_open").desc())
)
