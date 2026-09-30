from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) & (F.col("event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")))
    .select("ticket_id")
)

ticket_side = (
    df_tickets
    .filter((F.col("priority") == "P4") & (F.col("closed_at") >= F.lit("2025-02-08 15:00:00").cast("timestamp")))
    .select(F.col("requester_id").alias("ticket_id"))
)

result = (
    event_side
    .intersectAll(ticket_side)
    .orderBy("ticket_id")
)
