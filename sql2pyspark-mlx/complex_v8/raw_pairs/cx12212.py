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

scored = (
    joined_ranges
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "hours_since_open",
        F.when(F.col("hours_since_open") >= 50.4, "A").when(F.col("hours_since_open") >= 28.8, "B").otherwise("C").alias("hours_since_open_tier"),
    )
)

with_details_ticket = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.hours_since_open",
        "s.hours_since_open_tier",
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_ticket
    .filter(F.upper(F.trim(F.col("agent_name"))).like("H%"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "ticket_csat",
        "agent_team",
    )
    .orderBy(F.col("event_at").desc(), F.col("event_type").desc())
)
