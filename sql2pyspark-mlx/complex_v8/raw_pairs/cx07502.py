from pyspark.sql import functions as F

agent_rows = df_support_agents.select("id", "hired_on", F.col("tier").alias("name"))

agent_side = (
    df_support_agents
    .filter(F.col("tier") == "T1")
    .select("id", "hired_on", "agent_name")
)

result = (
    agent_rows
    .union(agent_side)
    .orderBy(F.col("hired_on").desc())
)
