from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .filter(F.col("tier").isin("T1", "T3"))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.when(F.col("team").isin("VIP", "Onboarding"), "group_a").when(F.col("team") == "Tech", "group_b").otherwise("other").alias("team_group"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "team",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("team_group")), " ", "_")).alias("team_group_slug"),
        F.concat_ws("-", F.col("agent_name"), F.col("team")).alias("agent_name_team_label"),
    )
)

tagged = (
    derived
    .filter(F.col("agent_name_team_label").isNotNull())
    .select(
        "*",
        F.substring(F.trim(F.col("team_group_slug")), 1, 2).alias("team_group_slug_prefix"),
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

result = (
    tagged
    .rollup("team", "team_or_unknown")
    .agg(
        F.grouping("team").alias("is_total_team"),
        F.sum(F.when(F.col("team").isin("Onboarding", "Billing", "VIP"), 1).otherwise(0)).alias("n_team_in_onboarding_billing_v"),
    )
)
