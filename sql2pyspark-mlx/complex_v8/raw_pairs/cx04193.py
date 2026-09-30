from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.length(F.trim(F.col("agent_name"))) >= 8) & (F.col("hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-02-26").cast("date"))))
    .select(F.col("tier").alias("name"))
)

agent_side = df_support_agents.select(F.col("agent_name").alias("name"))

result = agent_rows.intersect(agent_side)
