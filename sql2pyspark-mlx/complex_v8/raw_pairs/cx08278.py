from pyspark.sql import functions as F

event_side = df_ticket_events.select("id", F.col("ticket_id").alias("qty"))

event_rows = df_ticket_events.select(F.col("ticket_id").alias("id"), F.col("id").alias("qty"))

result = (
    event_side
    .unionByName(event_rows)
    .orderBy("id")
)
