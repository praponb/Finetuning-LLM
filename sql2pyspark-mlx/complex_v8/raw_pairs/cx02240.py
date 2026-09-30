from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("csat").alias("key_id"), "priority")

ticket_rows = df_tickets.select(F.col("requester_id").alias("key_id"), "priority")

result = ticket_side.subtract(ticket_rows)
