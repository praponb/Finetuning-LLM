from pyspark.sql import functions as F

filtered_agents = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

agent_side = (
    filtered_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 7)
    .select("tier", F.col("id").alias("qty"), F.col("hired_on").alias("day"))
)

agent_side_2 = df_support_agents.select("team", "id", "hired_on")

result = (
    agent_side
    .union(agent_side_2)
    .distinct()
    .orderBy("qty", "day")
)
