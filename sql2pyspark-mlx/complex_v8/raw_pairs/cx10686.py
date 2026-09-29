from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .select(
        F.lit("agent").alias("src"),
        "agent_name",
        "tier",
        "hired_on",
    )
)

agent_side_2 = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("team") != "VIP"))
    .select(
        F.lit("agent_2").alias("src"),
        "team",
        "tier",
        "hired_on",
    )
)

result = (
    agent_side
    .union(agent_side_2)
    .orderBy("hired_on", "src")
)
