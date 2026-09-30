from pyspark.sql import functions as F

joined_ranges = (
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

combined_details_agent = (
    joined_ranges.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

combined_details = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

detail_rows_tickets = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.agent_name",
        "s.agent_tier_2",
        "s.agent_agent_name",
        "s.agent_hired_on",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    detail_rows_tickets
    .filter(F.upper(F.trim(F.col("agent_agent_name"))).like("G%"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "agent_id",
        "agent_name",
        "agent_tier_2",
        "agent_agent_name",
        "agent_hired_on",
        "ticket_id_2",
        "ticket_opened_at",
        "ticket_priority",
        "ticket_csat",
    )
    .orderBy(F.col("ticket_id_2").desc(), F.col("id").desc())
)
