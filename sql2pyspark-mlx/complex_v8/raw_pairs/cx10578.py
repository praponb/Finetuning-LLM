from pyspark.sql import functions as F

agent_rows = df_support_agents.select(F.col("tier").alias("name"))

agent_rows_2 = (
    df_support_agents
    .filter((F.col("tier") == "T2") & (F.year(F.col("hired_on")) == 2024))
    .select("team")
)

result = agent_rows.intersectAll(agent_rows_2)
