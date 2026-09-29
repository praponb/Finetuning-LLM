from pyspark.sql import functions as F

recent_agents = (
    df_support_agents
    .filter(F.upper(F.trim(F.col("agent_name"))).like("G%"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
        F.round(F.avg("csat"), 2).alias("avg_ticket_csat"),
    )
)

profiled = (
    recent_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.avg_ticket_csat",
    )
)

result = (
    profiled
    .groupBy("tier")
    .pivot("team", ["VIP", "Onboarding", "Tech", "Billing"])
    .agg(F.min("agent_name_len").alias("min_v"), F.max("agent_name_len").alias("max_v"))
    .orderBy("tier")
)
