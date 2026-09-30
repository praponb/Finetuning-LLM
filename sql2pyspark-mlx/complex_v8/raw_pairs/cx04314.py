from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(8, 10))
    .select(
        F.col("requester_id").alias("ref_id"),
        "closed_at",
        F.lit("ticket").alias("source"),
    )
)

event_side = df_ticket_events.select("ticket_id", "event_at", F.lit("event").alias("source"))

event_rows = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 14)
    .select("ticket_id", "event_at", F.lit("event").alias("source"))
)

result = (
    ticket_rows
    .union(event_side)
    .union(event_rows)
)
