from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "ticket_id",
        F.col("event_at").alias("happened_at"),
        F.col("id").alias("entity_id"),
    )
)

event_side = (
    df_ticket_events
    .select(
        "ticket_id",
        F.col("event_at").alias("happened_at"),
        F.col("id").alias("entity_id"),
    )
)

event_side_2 = (
    df_ticket_events
    .select(
        F.col("id").alias("ticket_id"),
        F.col("event_at").alias("happened_at"),
        F.col("ticket_id").alias("entity_id"),
    )
)

result = (
    event_rows
    .union(event_side)
    .union(event_side_2)
    .orderBy("ticket_id")
)
