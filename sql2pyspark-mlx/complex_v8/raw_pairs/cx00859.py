from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter((~F.col("team").isin("Billing", "Tech")) & (F.upper(F.trim(F.col("agent_name"))).like("H%")))
    .select(F.col("id").alias("entity_id"), "tier")
)

agent_rows_2 = df_support_agents.select(F.col("id").alias("entity_id"), "tier")

result = (
    agent_rows
    .intersect(agent_rows_2)
    .orderBy(F.col("entity_id").desc())
)
