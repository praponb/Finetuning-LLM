from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.col("agent_name").isNotNull())
    .select(F.col("team").alias("label"), "id", "hired_on")
)

agent_side = (
    df_support_agents
    .filter(~F.col("tier").isin("T1", "T2"))
    .select("agent_name", "id", "hired_on")
)

agent_side_2 = df_support_agents.select("tier", "id", "hired_on")

result = (
    agent_rows
    .union(agent_side)
    .union(agent_side_2)
)
