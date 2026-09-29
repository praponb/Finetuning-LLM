from pyspark.sql import functions as F

recent = (
    df_support_agents
    .filter((~F.col("team").isin("Billing", "Onboarding")) & (F.col("hired_on") >= F.lit("2024-05-02").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.when(F.col("team").isin("Billing", "Onboarding"), "group_a").when(F.col("team") == "Tech", "group_b").otherwise("other").alias("team_group"),
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
    )
)

screened = (
    recent
    .crossJoin(recent.agg(F.max("agent_name_len").alias("overall_max_agent_name_len")))
    .filter(F.col("agent_name_len") < F.col("overall_max_agent_name_len"))
    .drop("overall_max_agent_name_len")
)

screened_2 = (
    screened.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("requester_id"), 2).alias("avg_ticket_requester_id"))
)

with_stats = (
    screened_2.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.avg_ticket_requester_id")
)

result = (
    with_stats
    .cube("team_group", "team")
    .agg(
        F.grouping("team_group").alias("is_total_team_group"),
        F.max("avg_ticket_requester_id").alias("max_avg_ticket_requester_id"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("H%"), F.col("agent_name_len")).otherwise(0)).alias("agent_name_len_upper_trim_agent_name_like_h"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("team_group"), F.lit("ALL")).alias("team_group"),
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "is_total_team_group",
        "max_avg_ticket_requester_id",
        "agent_name_len_upper_trim_agent_name_like_h",
        "n_rows",
    )
    .orderBy(F.col("team_group").asc_nulls_last(), F.col("team").asc_nulls_last())
)
