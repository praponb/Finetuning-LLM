from pyspark.sql import functions as F

filtered_agents = (
    df_support_agents
    .filter((~F.col("tier").isin("T2", "T3")) & (F.col("hired_on") < F.lit("2025-02-27").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.when(F.col("team").isin("VIP", "Tech"), "group_a").when(F.col("team") == "Billing", "group_b").otherwise("other").alias("team_group"),
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
    )
)

tagged = (
    filtered_agents
    .select(
        "id",
        "agent_name",
        "hired_on",
        "team_group",
        "agent_name_len",
        "days_since_hired_on",
        F.when(F.col("team").isin("Onboarding", "Billing"), "group_a").when(F.col("team") == "Tech", "group_b").otherwise("other").alias("team_group_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("csat").alias("max_ticket_csat"),
        F.sum("requester_id").alias("total_ticket_requester_id"),
    )
)

profiled = (
    tagged.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_csat",
        "t.total_ticket_requester_id",
    )
)

without_matches = (
    profiled
    .crossJoin(profiled.agg(F.avg("agent_name_len").alias("overall_avg_agent_name_len")))
    .filter(F.col("agent_name_len") > F.col("overall_avg_agent_name_len"))
    .drop("overall_avg_agent_name_len")
)

screened = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (~F.col("t.priority").isin("P4", "P1")) & (F.col("t.closed_at") >= F.lit("2025-02-02 05:00:00").cast("timestamp")), "left_semi")
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count_2"))
)

profiled_2 = (
    screened.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

result = (
    profiled_2
    .select(
        "id",
        "hired_on",
        "team_group",
        "agent_name_len",
        "team_group_2",
        "ticket_count",
        "max_ticket_csat",
        "total_ticket_requester_id",
        "ticket_count_2",
    )
    .orderBy(F.col("hired_on").asc_nulls_last(), "id")
    .limit(20)
)
