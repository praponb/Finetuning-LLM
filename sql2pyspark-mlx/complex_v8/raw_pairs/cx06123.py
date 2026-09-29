from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("event_type") != "escalated")
    .select("ticket_id", "event_at", F.lit("event").alias("origin"))
)

ticket_rows = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")) & (F.col("csat") < 4))
    .select("requester_id", "opened_at", F.lit("ticket").alias("origin"))
)

event_side_2 = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "reopened")) & (F.col("event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp")))
    .select("id", "event_at", F.lit("event_3").alias("origin"))
)

unioned = (
    event_side
    .union(ticket_rows)
    .union(event_side_2)
)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ticket_id").alias("total_ticket_id"),
        F.max("event_at").alias("latest_event_at"),
    )
    .orderBy("origin")
)
