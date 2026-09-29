from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter(F.col("event_type") != "assigned")
    .select("ticket_id")
)

event_side = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 11) & (F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
    .select(F.col("id").alias("ticket_id"))
)

result = event_rows.intersectAll(event_side)
