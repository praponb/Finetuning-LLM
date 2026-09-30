from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%ng%")) & (F.col("tier").isin("T2", "T1")))
    .select("id")
)

agent_rows = df_support_agents.select("id")

result = (
    agent_side
    .subtract(agent_rows)
    .orderBy(F.col("id").desc())
)
