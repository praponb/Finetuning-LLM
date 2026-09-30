from pyspark.sql import functions as F

agent_rows = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 5)
    .select(F.col("id").alias("key_id"))
)

ticket_rows = df_tickets.select(F.col("id").alias("key_id"))

result = agent_rows.exceptAll(ticket_rows)
