from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.year(F.col("hired_on")) == 2024)
    .select("tier")
)

agent_side = df_support_agents.select("tier")

result = (
    agent_rows
    .intersect(agent_side)
    .orderBy("tier")
)
