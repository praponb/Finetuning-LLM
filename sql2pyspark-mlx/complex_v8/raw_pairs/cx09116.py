from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("channel").alias("name"))

agent_side = df_support_agents.select(F.col("team").alias("name"))

result = (
    ticket_side
    .subtract(agent_side)
    .orderBy(F.col("name").desc())
)
