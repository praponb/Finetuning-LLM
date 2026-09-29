from pyspark.sql import functions as F

event_rows = df_ticket_events.select(F.col("ticket_id").alias("ref_id"))

ticket_side = df_tickets.select("agent_id")

result = (
    event_rows
    .exceptAll(ticket_side)
    .orderBy(F.col("ref_id").desc())
)
