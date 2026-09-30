from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("csat") >= 2) & (F.hour(F.col("closed_at")).between(6, 10)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
    )
)

joined_agent = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

combined_details_agent = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

tagged = (
    combined_details_agent
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.date_format(F.col("closed_at"), "MMM yyyy").alias("closed_at_label"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

enriched = (
    tagged
    .filter(F.col("priority") == "P3")
    .select(
        "*",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.when(F.col("days_since_closed_at") >= 595, "HIGH").when(F.col("days_since_closed_at") >= 315, "MEDIUM").otherwise("LOW").alias("days_since_closed_at_tier"),
    )
)

with_children = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("reopened", "resolved", "replied")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.days_since_closed_at",
        "s.channel_prefix",
        "s.closed_at_plus_30m",
        "s.closed_at_year",
        "s.days_since_closed_at_tier",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    with_children
    .select(
        "id",
        "closed_at",
        "priority",
        "days_since_closed_at",
        "channel_prefix",
        "closed_at_plus_30m",
        "closed_at_year",
        "event_at",
        "event_type",
        F.abs(F.col("closed_at_year") - 2025).alias("closed_at_year_distance"),
    )
    .orderBy(F.col("priority").asc())
)
