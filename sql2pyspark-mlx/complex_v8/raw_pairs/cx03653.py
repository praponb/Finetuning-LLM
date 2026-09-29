from pyspark.sql import functions as F

event_rows = df_ticket_events.select("id")

ticket_side = df_tickets.select(F.col("agent_id").alias("id"))

result = (
    event_rows
    .intersect(ticket_side)
    .orderBy(F.col("id").desc())
)
