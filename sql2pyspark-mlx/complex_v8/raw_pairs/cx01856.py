from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("team") == "VIP"))
    .select(F.lit("agent").alias("src"), "id", F.col("tier").alias("name"))
)

agent_side = df_support_agents.select(F.lit("agent_2").alias("src"), "id", "agent_name")

result = (
    agent_rows
    .union(agent_side)
    .orderBy("id")
)
