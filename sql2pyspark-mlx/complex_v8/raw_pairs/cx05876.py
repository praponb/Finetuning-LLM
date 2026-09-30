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

per_group = (
    joined_ranges
    .filter(~F.col("priority").isin("P2", "P1"))
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"), "ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.round(F.avg("hours_since_open"), 2).alias("avg_hours_since_open"),
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open_check"),
    )
    .filter(F.col("max_hours_since_open_check") > 25.2)
    .drop("max_hours_since_open_check")
)

with_parent = (
    per_group.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.event_at_month",
        "s.ticket_id",
        "s.last_event_at",
        "s.avg_hours_since_open",
        "s.min_hours_since_open",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_parent_ticket = (
    with_parent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

result = (
    with_parent_ticket
    .select(
        "event_at_month",
        "last_event_at",
        "min_hours_since_open",
        "agent_tier",
        "ticket_requester_id",
        F.date_format(F.col("event_at_month"), "yyyy-MM-dd HH:00").alias("event_at_month_label"),
    )
    .orderBy(F.col("event_at_month_label").desc(), "agent_tier")
)
