from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("tier") != "T2") & (F.col("hired_on") < F.lit("2025-02-26").cast("date")))
    .select(F.col("agent_name").alias("category"))
)

agent_side_2 = df_support_agents.select(F.col("team").alias("category"))

result = (
    agent_side
    .subtract(agent_side_2)
    .orderBy("category")
)
