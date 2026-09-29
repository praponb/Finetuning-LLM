from pyspark.sql import functions as F

base_agents = df_support_agents.filter(F.col("team").isin("VIP", "Onboarding", "Billing"))

agent_side = base_agents.select(F.col("agent_name").alias("category"), "hired_on")

agent_side_2 = df_support_agents.select(F.col("agent_name").alias("category"), "hired_on")

combined = agent_side.unionByName(agent_side_2)

result = (
    combined
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("hired_on").alias("latest_hired_on"),
    )
    .orderBy("category")
)
