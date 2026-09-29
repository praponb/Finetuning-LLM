from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .select(F.col("id").alias("ref_id"), "hired_on", F.col("team").alias("label"))
)

agent_rows = (
    df_support_agents
    .filter(~F.col("team").isin("Billing", "Onboarding"))
    .select(F.col("id").alias("ref_id"), "hired_on", F.col("tier").alias("label"))
)

result = agent_side.unionByName(agent_rows)
