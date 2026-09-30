from pyspark.sql import functions as F

cleaned_agents = (
    df_support_agents
    .filter((F.col("hired_on").between(F.lit("2024-01-30").cast("date"), F.lit("2025-03-30").cast("date"))) | (F.col("tier").isin("T3", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.substring(F.trim(F.col("agent_name")), 1, 3).alias("agent_name_prefix"),
    )
)

grouped = (
    cleaned_agents
    .groupBy("tier", "team")
    .agg(
        F.count(F.when(F.col("team").isNotNull(), 1)).alias("n_team_is_not_null"),
        F.sum(F.when(F.lower(F.col("agent_name")).like("%s %"), 1).otherwise(0)).alias("n_lower_agent_name_like_s"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("tier").isNotNull(), 1)).alias("n_tier_is_not_null"),
    )
)

result = (
    grouped
    .filter(F.col("n_team_is_not_null") < 9)
    .select(
        "tier",
        "team",
        "n_team_is_not_null",
        "n_lower_agent_name_like_s",
        "distinct_ids",
    )
    .orderBy(F.col("team").desc())
)
