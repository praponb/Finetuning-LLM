from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("csat") >= 1) & (F.col("closed_at") < F.lit("2025-03-05 00:00:00").cast("timestamp")))
    .select("id")
)

event_side = (
    df_ticket_events
    .filter((F.col("event_type").isin("escalated", "replied")) & (F.col("ticket_id").isNotNull()))
    .select("ticket_id")
)

result = (
    ticket_side
    .exceptAll(event_side)
    .orderBy("id")
)
