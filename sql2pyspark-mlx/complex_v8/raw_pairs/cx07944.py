from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("priority") == "P3")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

enriched_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.days_since_closed_at",
        "su.agent_name",
    )
)

agent_id_stats = (
    enriched_agent
    .groupBy("agent_id")
    .agg(F.avg("days_since_closed_at").alias("avg_days_since_closed_at"))
)

above_average = (
    enriched_agent.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_closed_at") > F.col("g.avg_days_since_closed_at"))
    .select(
        "s.*",
        "g.avg_days_since_closed_at",
        F.round(F.col("s.days_since_closed_at") - F.col("g.avg_days_since_closed_at"), 2).alias("days_since_closed_at_minus_avg"),
    )
)

result = (
    above_average
    .groupBy("agent_id")
    .pivot("channel", ["web", "email"])
    .agg(F.min("days_since_closed_at_minus_avg"))
    .orderBy("agent_id")
)
