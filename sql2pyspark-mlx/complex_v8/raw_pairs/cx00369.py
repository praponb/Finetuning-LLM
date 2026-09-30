from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-19 10:00:00").cast("timestamp"))
    .select(F.col("requester_id").alias("key_id"))
)

event_side = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(6, 9)) & (F.col("ticket_id") >= 33))
    .select("ticket_id")
)

result = (
    ticket_side
    .subtract(event_side)
    .orderBy("key_id")
)
