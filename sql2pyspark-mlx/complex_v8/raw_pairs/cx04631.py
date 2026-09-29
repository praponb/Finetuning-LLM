from pyspark.sql import functions as F

ticket_rows = df_tickets.select(F.col("agent_id").alias("key_id"), "channel")

event_side = df_ticket_events.select(F.col("id").alias("key_id"), F.col("actor").alias("channel"))

result = (
    ticket_rows
    .subtract(event_side)
    .orderBy(F.col("channel").desc(), "key_id")
)
