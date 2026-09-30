from pyspark.sql import functions as F

agent_side = df_support_agents.select(F.col("tier").alias("label"))

ticket_side = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "outage"))
    .select(F.col("channel").alias("label"))
)

result = agent_side.subtract(ticket_side)
