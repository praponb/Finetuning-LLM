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

enriched = (
    joined_ranges
    .filter(F.col("agent_id") <= 6)
    .select(
        "id",
        "event_at",
        "priority",
        "agent_id",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "phone") & (F.col("t.csat") <= 4), "left_anti")
    .filter(F.col("priority_group") != "group_a")
)

result = (
    with_activity
    .select(
        "id",
        "event_at",
        "priority",
        "agent_id",
        "event_at_day",
        "event_at_label",
        "priority_group",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at_day")).alias("days_since_event_at_day"),
    )
    .orderBy(F.col("id").desc())
)
