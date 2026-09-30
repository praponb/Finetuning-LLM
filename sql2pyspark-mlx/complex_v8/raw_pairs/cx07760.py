from pyspark.sql import functions as F

agent_side = df_support_agents.select(F.col("id").alias("qty"), "team")

agent_side_2 = df_support_agents.select("id", "agent_name")

result = agent_side.subtract(agent_side_2)
