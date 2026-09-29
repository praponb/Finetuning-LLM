from pyspark.sql import functions as F

ticket_side = df_tickets.select("csat")

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 37) & (F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
    .select("id")
)

result = ticket_side.intersect(event_rows)
