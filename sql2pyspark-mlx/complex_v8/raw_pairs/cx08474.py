from pyspark.sql import functions as F

in_window = (
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
    in_window
    .select(
        "*",
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.month(F.col("t.opened_at")).isin(2, 3)), "left_semi")
)

result = (
    without_matches
    .filter(F.col("hours_since_open") <= 57.6)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "priority_group",
        "days_since_event_at",
        F.concat(F.col("priority"), F.lit("#"), F.col("days_since_event_at").cast("string")).alias("priority_ref"),
    )
    .orderBy(F.col("agent_id").desc())
)
