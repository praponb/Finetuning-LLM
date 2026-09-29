from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date"))
    .select("ticket_id", "id")
)

ticket_side = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) & (F.col("closed_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select("requester_id", "agent_id")
)

stacked = event_side.union(ticket_side)

result = (
    stacked
    .groupBy("ticket_id")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("ticket_id")
)
