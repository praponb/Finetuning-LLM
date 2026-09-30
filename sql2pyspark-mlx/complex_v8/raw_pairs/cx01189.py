from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        F.col("id").alias("entity_id"),
        "event_at",
        F.lit("event").alias("source"),
    )
)

event_side_2 = (
    df_ticket_events
    .select(
        F.col("ticket_id").alias("entity_id"),
        "event_at",
        F.lit("event_2").alias("source"),
    )
)

result = (
    event_side
    .union(event_side_2)
    .orderBy(F.col("entity_id").desc())
)
