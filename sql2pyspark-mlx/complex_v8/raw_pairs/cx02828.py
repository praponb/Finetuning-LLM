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

with_details = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team").isin("Billing", "VIP"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_details
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.last_day(F.col("agent_hired_on")).alias("agent_hired_on_month_end"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_team",
        "agent_tier",
        "agent_hired_on",
        "hours_since_open_bucket",
        "event_at_label",
        F.trunc(F.col("agent_hired_on_month_end"), "MM").alias("agent_hired_on_month_end_month_start"),
    )
    .orderBy("agent_team")
)
