from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter((F.length(F.trim(F.col("agent_name"))) >= 5) & (F.col("team") == "Tech"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    valid_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.closed_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.array_contains(F.col("t.tags"), "login")), "left_semi")
)

tier_stats = (
    filtered
    .groupBy("tier")
    .agg(F.avg("ticket_count").alias("avg_ticket_count"))
)

outliers = (
    filtered.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.ticket_count") > F.col("g.avg_ticket_count"))
    .select(
        "s.*",
        "g.avg_ticket_count",
        F.round(F.col("s.ticket_count") - F.col("g.avg_ticket_count"), 2).alias("ticket_count_minus_avg"),
    )
)

result = (
    outliers
    .cube("team", "tier_or_unknown")
    .agg(
        F.count(F.when(F.col("team").isin("VIP", "Tech"), 1)).alias("n_team_in_vip_tech"),
        F.max("avg_ticket_count").alias("max_avg_ticket_count"),
    )
    .orderBy(
        F.col("team").asc_nulls_last(),
        F.col("tier_or_unknown").asc_nulls_last(),
    )
)
