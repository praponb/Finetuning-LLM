from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter(F.col("team").isin("Tech", "Billing"))
    .select("id", "hired_on")
)

agent_side_2 = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (F.year(F.col("hired_on")) == 2024))
    .select("id", "hired_on")
)

combined = agent_side.union(agent_side_2)

result = (
    combined
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("hired_on").alias("latest_hired_on"),
    )
    .orderBy("id")
)
