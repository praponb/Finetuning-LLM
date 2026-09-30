from pyspark.sql import functions as F

agent_rows = df_support_agents.select("id")

agent_side = df_support_agents.select("id")

result = agent_rows.intersect(agent_side)
