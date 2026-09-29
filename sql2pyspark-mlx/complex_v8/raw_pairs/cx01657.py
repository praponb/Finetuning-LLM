from pyspark.sql import functions as F

recent_agents = (
    df_support_agents
    .filter(F.col("tier").isin("T1", "T2"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.when(F.col("team").isin("Billing", "Tech"), "group_a").when(F.col("team") == "Onboarding", "group_b").otherwise("other").alias("team_group"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("csat").alias("total_ticket_csat"),
        F.max("csat").alias("max_ticket_csat"),
    )
)

with_stats = (
    recent_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.total_ticket_csat", "t.max_ticket_csat")
)

line_level_tickets = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.tier",
        "s.team_group",
        "s.total_ticket_csat",
        "s.max_ticket_csat",
        F.col("t.id").alias("ticket_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    line_level_tickets
    .filter(F.size(F.col("ticket_tags")) >= 2)
    .select(
        "id",
        "tier",
        "team_group",
        "ticket_id",
        "ticket_tags",
        F.coalesce(F.col("team_group"), F.lit("UNKNOWN")).alias("team_group_or_unknown"),
        F.upper(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
    .orderBy(F.col("tier").desc())
)
