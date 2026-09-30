from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((F.col("hired_on") < F.lit("2025-02-26").cast("date")) & (~F.col("team").isin("Tech", "Onboarding")))
    .select("tier", F.col("hired_on").alias("day"), "id")
)

agent_rows_2 = (
    df_support_agents
    .filter((F.col("team").isin("VIP", "Onboarding", "Tech")) & (F.col("tier") == "T1"))
    .select("team", "hired_on", "id")
)

combined = agent_rows.union(agent_rows_2)

result = (
    combined
    .groupBy("tier")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("tier")
)
