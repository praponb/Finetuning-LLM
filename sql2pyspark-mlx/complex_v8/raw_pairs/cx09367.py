from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("tier").isin("T1", "T2")) & (F.col("team").isin("Tech", "Onboarding")))
    .select(F.col("id").alias("qty"), F.col("team").alias("label"))
)

ticket_side = df_tickets.select("agent_id", "channel")

result = agent_side.subtract(ticket_side)
