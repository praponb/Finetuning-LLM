from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

filtered = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.team").isin("Tech", "VIP")) & (F.col("su.tier").isNotNull()), "left_semi")
)

screened = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.length(F.trim(F.col("su.agent_name"))) >= 5) & (F.year(F.col("su.hired_on")) == 2025), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
    .orderBy(F.col("ticket_id").desc(), "event_type")
)
