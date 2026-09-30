from pyspark.sql import functions as F

in_window = (
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

combined_details = (
    in_window.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

labeled = (
    combined_details
    .select(
        "*",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "agent_name",
        "agent_tier",
        "agent_team",
        "ticket_priority",
        "ticket_opened_at",
        (F.col("hours_since_open") - F.col("hours_since_open_filled")).alias("hours_since_open_minus_hours_since_open_filled"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
    .orderBy(F.col("event_type").desc())
)
