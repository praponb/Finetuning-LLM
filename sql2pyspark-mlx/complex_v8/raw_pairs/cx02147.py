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

tagged = (
    matched
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        "event_type",
        "agent_id",
        "hours_since_open",
        "hours_since_open_bucket",
        "event_at_year",
    )
    .orderBy(F.col("agent_id").asc())
)
