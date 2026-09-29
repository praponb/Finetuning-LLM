from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("csat").between(2, 4))
    .select(F.col("id").alias("key_id"), F.col("priority").alias("name"))
)

event_side = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(F.col("id").alias("key_id"), F.col("event_type").alias("name"))
)

result = (
    ticket_side
    .intersect(event_side)
    .orderBy(F.col("name").desc())
)
