from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") >= F.lit("2024-05-02").cast("date")) & (F.col("team").isNotNull()))
    .select(F.lit("agent").alias("origin"), "tier", F.col("hired_on").alias("day"))
)

agent_rows = (
    df_support_agents
    .filter(F.col("team").isin("Tech", "Onboarding"))
    .select(F.lit("agent_2").alias("origin"), "team", "hired_on")
)

result = (
    agent_side
    .union(agent_rows)
    .distinct()
    .orderBy(F.col("day").desc(), "tier")
)
