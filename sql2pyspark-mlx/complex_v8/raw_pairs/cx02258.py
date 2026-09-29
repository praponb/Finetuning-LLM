from pyspark.sql import functions as F

matched = (
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

joined = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.agent_id",
        "s.hours_since_open",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    joined
    .filter(F.col("hours_since_open") > 14.4)
    .select(
        "id",
        "ticket_id",
        "agent_id",
        "hours_since_open",
        "agent_name",
        F.length(F.trim(F.col("agent_tier"))).alias("agent_tier_len"),
        F.abs(F.col("hours_since_open") - 25.2).alias("hours_since_open_distance"),
    )
)
