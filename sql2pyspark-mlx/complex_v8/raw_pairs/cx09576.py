from pyspark.sql import functions as F

recent = df_ticket_events.filter((F.col("event_type") == "created") & (F.col("actor") != "system"))

event_side = recent.select(F.col("event_type").alias("category"))

ticket_rows = df_tickets.select("priority")

result = event_side.subtract(ticket_rows)
