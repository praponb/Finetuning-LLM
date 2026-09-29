from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.col("requester_id").between(3, 20)) & (F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.id", "s.agent_id", F.col("su.team").alias("agent_team"))
)

result = (
    enriched
    .groupingSets(
        [["agent_id", "agent_team"], ["agent_id"]],
        "agent_id", "agent_team",
    )
    .agg(
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("agent_team") == "Onboarding", 1).otherwise(0)).alias("n_agent_team_onboarding"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        "agent_team_list",
        "n_rows",
        "n_agent_team_onboarding",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)
