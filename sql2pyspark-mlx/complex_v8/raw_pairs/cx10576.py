from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-27").cast("date")) & (F.col("agent_name").isNotNull()))
    .select(F.lit("agent").alias("src"), "id", "hired_on")
)

agent_rows_2 = df_support_agents.select(F.lit("agent_2").alias("src"), "id", "hired_on")

result = (
    agent_rows
    .union(agent_rows_2)
    .distinct()
    .orderBy("id")
)
