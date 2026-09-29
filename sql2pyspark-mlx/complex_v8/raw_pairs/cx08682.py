from pyspark.sql import functions as F

covered = (
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

with_details = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_parent = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        "s.hours_since_open",
        "s.ticket_opened_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
    )
)

joined_agent = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        "s.hours_since_open",
        "s.agent_tier",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier_2"),
        "su.agent_name",
    )
)

result = (
    joined_agent
    .select(
        "id",
        "agent_id",
        "hours_since_open",
        "agent_tier",
        "agent_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
    )
    .orderBy(F.col("agent_id").asc())
)
