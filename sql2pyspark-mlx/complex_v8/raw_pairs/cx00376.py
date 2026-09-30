from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(
        F.col("actor").alias("label"),
        F.col("ticket_id").alias("key_id"),
        F.lit("event").alias("origin"),
    )
)

ticket_rows = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("requester_id") >= 4))
    .select("channel", "csat", F.lit("ticket").alias("origin"))
)

result = event_side.union(ticket_rows)
