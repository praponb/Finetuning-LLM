from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .select(F.col("id").alias("entity_id"), F.col("agent_name").alias("name"))
)

agent_side = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("%en%"))
    .select(F.col("id").alias("entity_id"), F.col("team").alias("name"))
)

result = agent_rows.unionByName(agent_side)
