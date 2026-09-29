from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("H%")) & (F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-02-26").cast("date"))))
    .select(F.col("id").alias("ref_id"), F.col("hired_on").alias("day"))
)

agent_rows = df_support_agents.select("id", "hired_on")

result = (
    agent_side
    .union(agent_rows)
    .orderBy("day")
)
