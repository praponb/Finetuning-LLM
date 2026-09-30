from pyspark.sql import functions as F

agent_side = df_support_agents.select("agent_name", "hired_on")

agent_rows = (
    df_support_agents
    .filter(F.col("team") == "Tech")
    .select("agent_name", "hired_on")
)

result = agent_side.union(agent_rows)
