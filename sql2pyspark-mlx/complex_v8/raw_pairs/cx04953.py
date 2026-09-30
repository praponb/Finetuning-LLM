from pyspark.sql import functions as F

ticket_side = df_tickets.select("csat", F.col("id").alias("key_id"), "priority")

ticket_rows = df_tickets.select("agent_id", "id", "priority")

result = (
    ticket_side
    .union(ticket_rows)
    .orderBy(F.col("csat").desc(), "priority")
)
