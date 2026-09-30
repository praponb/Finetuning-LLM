from pyspark.sql import functions as F

joined_ranges = (
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

derived = (
    joined_ranges
    .filter(F.col("agent_id") <= 9)
    .withColumn("priority_group", F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other"))
)

annotated = (
    derived
    .filter(F.col("ticket_id") >= 53)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("priority_group")), " ", "_")).alias("priority_group_slug"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "hours_since_open",
        "priority_group_slug",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
    .orderBy("event_at_plus_2h")
)
