from pyspark.sql import functions as F

agent_rows = df_support_agents.select("id", F.col("agent_name").alias("category"))

agent_side = (
    df_support_agents
    .filter((F.col("hired_on") >= F.lit("2024-07-28").cast("date")) & (F.lower(F.col("agent_name")).like("%is%")))
    .select("id", "tier")
)

result = (
    agent_rows
    .subtract(agent_side)
    .orderBy(F.col("category").desc())
)
