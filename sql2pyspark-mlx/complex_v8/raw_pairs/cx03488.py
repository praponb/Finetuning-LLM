from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .select(F.col("id").alias("entity_id"), F.col("hired_on").alias("event_date"))
)

agent_side = (
    df_support_agents
    .filter(F.col("agent_name").isNotNull())
    .select("id", "hired_on")
)

result = agent_rows.union(agent_side)
