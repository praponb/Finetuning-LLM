from pyspark.sql import functions as F

joined_ranges = (
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

scored = (
    joined_ranges
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.concat_ws("-", F.col("priority"), F.col("event_type")).alias("priority_event_type_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "priority_slug",
        "event_at_plus_2h",
        "priority_event_type_label",
    )
    .orderBy(F.col("priority_slug").desc(), "event_type")
)
