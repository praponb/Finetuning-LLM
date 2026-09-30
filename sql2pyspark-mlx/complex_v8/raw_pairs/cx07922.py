from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select("ticket_id", F.col("id").alias("key_id"))
)

event_side_2 = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "reopened", "replied"))
    .select("id", "ticket_id")
)

result = event_side.union(event_side_2)
