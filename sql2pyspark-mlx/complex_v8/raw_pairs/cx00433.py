from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(F.col("id").alias("key_id"))
)

event_side = df_ticket_events.select("ticket_id")

result = (
    ticket_side
    .subtract(event_side)
    .orderBy("key_id")
)
