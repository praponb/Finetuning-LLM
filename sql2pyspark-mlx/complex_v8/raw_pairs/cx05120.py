from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 3))
    .select(F.col("channel").alias("label"))
)

agent_side = df_support_agents.select(F.col("agent_name").alias("label"))

result = (
    ticket_rows
    .subtract(agent_side)
    .orderBy("label")
)
