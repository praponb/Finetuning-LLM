from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("requester_id") < 15)
    .select(
        F.col("priority").alias("label"),
        F.col("id").alias("key_id"),
        "channel",
        F.lit("ticket").alias("src"),
    )
)

event_side = (
    df_ticket_events
    .select(
        "actor",
        "id",
        "event_type",
        F.lit("event").alias("src"),
    )
)

result = (
    ticket_side
    .union(event_side)
    .orderBy("label", "key_id")
)
