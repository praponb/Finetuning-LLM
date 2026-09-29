from pyspark.sql import functions as F

covered = (
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

filtered = (
    covered
    .crossJoin(covered.agg(F.min("hours_since_open").alias("overall_min_hours_since_open")))
    .filter(F.col("hours_since_open") > F.col("overall_min_hours_since_open"))
    .drop("overall_min_hours_since_open")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "agent_id",
        "hours_since_open",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
    )
    .orderBy(F.col("ticket_id").desc_nulls_first(), F.col("agent_id").asc())
)
