from pyspark.sql import functions as F

agent_side = df_support_agents.select("id", "tier")

ticket_rows = df_tickets.select(F.col("requester_id").alias("id"), F.col("channel").alias("tier"))

result = agent_side.intersect(ticket_rows)
