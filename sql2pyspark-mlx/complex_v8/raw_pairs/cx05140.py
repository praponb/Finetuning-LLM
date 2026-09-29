from pyspark.sql import functions as F

agent_side = df_support_agents.select("id", "team", "hired_on")

agent_side_2 = df_support_agents.select("id", F.col("agent_name").alias("team"), "hired_on")

result = (
    agent_side
    .unionByName(agent_side_2)
    .distinct()
)
