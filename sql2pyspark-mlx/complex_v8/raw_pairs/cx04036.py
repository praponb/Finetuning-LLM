from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(
        F.col("ticket_id").alias("key_id"),
        F.col("event_at").alias("happened_at"),
        F.lit("event").alias("src"),
    )
)

ticket_rows = (
    df_tickets
    .select(
        F.col("requester_id").alias("key_id"),
        F.col("opened_at").alias("happened_at"),
        F.lit("ticket").alias("src"),
    )
)

ticket_side = (
    df_tickets
    .filter(F.col("csat") > 2)
    .select(
        F.col("csat").alias("key_id"),
        F.col("opened_at").alias("happened_at"),
        F.lit("ticket").alias("src"),
    )
)

result = (
    event_side
    .unionByName(ticket_rows)
    .unionByName(ticket_side)
    .orderBy("src")
)
