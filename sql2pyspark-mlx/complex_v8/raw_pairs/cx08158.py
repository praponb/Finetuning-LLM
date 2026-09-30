from pyspark.sql import functions as F

event_side = df_ticket_events.select("actor", "event_at")

event_side_2 = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(F.col("event_type").alias("actor"), "event_at")
)

result = event_side.unionByName(event_side_2)
