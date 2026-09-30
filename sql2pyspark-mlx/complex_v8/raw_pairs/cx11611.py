from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(
        F.col("ticket_id").alias("key_id"),
        F.col("event_at").alias("happened_at"),
        F.lit("event").alias("origin"),
    )
)

event_side_2 = df_ticket_events.select("id", "event_at", F.lit("event_2").alias("origin"))

result = event_side.union(event_side_2)
