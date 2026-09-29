from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

joined_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") != "T3")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.channel_len",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_parent_agent = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.channel_len",
        "s.agent_team",
        "s.agent_tier",
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

channel_stats = (
    with_parent_agent
    .groupBy("channel")
    .agg(F.avg("csat").alias("avg_csat"))
)

vs_group = (
    with_parent_agent.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

with_parent_agent_2 = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on") < F.lit("2025-02-27").cast("date"))
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.channel_len",
        "s.agent_team",
        "s.agent_hired_on_2",
        "s.csat_minus_avg",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

summary = (
    with_parent_agent_2
    .groupBy(
        "channel",
        F.when(F.col("csat_minus_avg") >= 2.5, "HIGH").when(F.col("csat_minus_avg") >= -2.5, "MEDIUM").otherwise("LOW").alias("csat_minus_avg_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.count(F.when(F.col("agent_team").isin("VIP", "Onboarding", "Billing"), 1)).alias("n_agent_team_in_vip_onboarding"),
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
    )
)

tagged = (
    summary
    .select(
        "*",
        F.substring(F.trim(F.col("csat_minus_avg_tier")), 1, 4).alias("csat_minus_avg_tier_prefix"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.concat(F.col("csat_minus_avg_tier"), F.lit("#"), F.col("row_count").cast("string")).alias("csat_minus_avg_tier_ref"),
    )
)

result = (
    tagged
    .cube("row_count", "channel_len", "channel")
    .agg(
        F.grouping("row_count").alias("is_total_row_count"),
        F.round(F.sum("channel_len") / F.nullif(F.sum("distinct_agent_teams"), F.lit(0)), 3).alias("channel_len_to_distinct_agent_teams_ratio"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("channel_len").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
