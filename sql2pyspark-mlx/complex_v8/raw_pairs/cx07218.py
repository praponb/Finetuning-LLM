from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select("requester_id", F.col("opened_at").alias("event_time"))
)

event_side = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        F.col("id").alias("requester_id"),
        F.col("event_at").alias("event_time"),
    )
)

result = ticket_side.union(event_side)
