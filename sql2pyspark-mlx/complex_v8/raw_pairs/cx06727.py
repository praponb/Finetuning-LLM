from pyspark.sql import functions as F

event_side = df_ticket_events.select("id")

ticket_rows = df_tickets.select(F.col("agent_id").alias("id"))

result = (
    event_side
    .subtract(ticket_rows)
    .orderBy(F.col("id").desc())
)
