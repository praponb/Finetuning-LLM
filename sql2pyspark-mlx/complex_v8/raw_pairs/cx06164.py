from pyspark.sql import functions as F

covered = (
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

combined_details = (
    covered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

joined_agent = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.length(F.trim(F.col("su.agent_name"))) >= 4), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    joined_agent
    .groupBy("ticket_closed_at_2", "ticket_closed_at")
    .pivot("event_type", ["escalated", "resolved", "replied", "assigned"])
    .agg(F.avg("hours_since_open").alias("avg_v"), F.max("hours_since_open").alias("max_v"))
    .orderBy("ticket_closed_at_2", "ticket_closed_at")
)
