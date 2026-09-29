from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("tier").isin("T3", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.hour(F.col("t.closed_at")).between(7, 10)) & (F.size(F.col("t.tags")) >= 2), "left_semi")
    .filter(F.col("team") == "Onboarding")
)

enriched = (
    filtered
    .filter(F.col("team").isNotNull())
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.lower(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
        F.when(F.col("team").isin("Tech", "VIP"), "group_a").when(F.col("team") == "Billing", "group_b").otherwise("other").alias("team_group"),
        F.concat_ws(": ", F.col("agent_name"), F.col("tier")).alias("agent_name_tier_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "team",
        "tier",
        "agent_name_clean",
        "team_group",
        F.length(F.trim(F.col("agent_name_tier_label"))).alias("agent_name_tier_label_len"),
        F.concat_ws("-", F.col("team"), F.col("tier")).alias("team_tier_label"),
    )
    .orderBy(F.col("team_group").desc(), F.col("id").asc())
)
