from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (~F.col("team").isin("Tech", "VIP")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.when(F.col("team").isin("VIP", "Onboarding"), "group_a").when(F.col("team") == "Billing", "group_b").otherwise("other").alias("team_group"),
    )
)

result = (
    scoped
    .select(
        "id",
        "team",
        "tier",
        "team_or_unknown",
        "team_group",
        F.length(F.trim(F.col("team_or_unknown"))).alias("team_or_unknown_len"),
    )
    .orderBy(F.col("team_or_unknown_len").desc(), F.col("team_or_unknown").desc())
)
