from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter((F.col("agent_name").isNotNull()) | (F.col("hired_on") >= F.lit("2024-07-09").cast("date")))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.concat(F.col("tier"), F.lit("#"), F.col("id").cast("string")).alias("tier_ref"),
    )
)

agent_side = (
    valid_agents
    .filter(F.col("hired_on") >= F.lit("2024-07-28").cast("date"))
    .select("tier_ref")
)

agent_side_2 = (
    df_support_agents
    .filter((F.col("hired_on").isNotNull()) & (~F.col("team").isin("Tech", "Billing")))
    .select(F.col("team").alias("tier_ref"))
)

result = agent_side.exceptAll(agent_side_2)
