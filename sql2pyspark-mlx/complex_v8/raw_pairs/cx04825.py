from pyspark.sql import functions as F

agent_rows = df_support_agents.select(F.col("id").alias("key_id"), F.col("hired_on").alias("day"))

agent_rows_2 = df_support_agents.select(F.col("id").alias("key_id"), F.col("hired_on").alias("day"))

result = (
    agent_rows
    .unionByName(agent_rows_2)
    .orderBy(F.col("key_id").desc(), "day")
)
