from pyspark.sql import functions as F

event_rows = df_ticket_events.select(F.col("event_type").alias("category"), "actor", "event_at")

ticket_side = df_tickets.select("priority", "channel", "closed_at")

result = (
    event_rows
    .union(ticket_side)
    .orderBy("event_at", "actor")
)
