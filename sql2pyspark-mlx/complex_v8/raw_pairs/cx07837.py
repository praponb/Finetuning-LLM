from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(F.col("id").alias("entity_id"), F.col("event_at").alias("event_time"))
)

ticket_side = (
    df_tickets
    .filter((F.col("csat") < 4) & (F.array_contains(F.col("tags"), "login")))
    .select(
        F.col("csat").alias("entity_id"),
        F.col("closed_at").alias("event_time"),
    )
)

result = (
    event_side
    .unionByName(ticket_side)
    .orderBy("entity_id")
)
