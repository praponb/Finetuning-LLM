from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .select(
        F.col("id").alias("key_id"),
        F.col("priority").alias("label"),
        F.col("agent_id").alias("entity_id"),
    )
)

event_rows = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 3))
    .select(
        F.col("id").alias("key_id"),
        F.col("actor").alias("label"),
        F.col("ticket_id").alias("entity_id"),
    )
)

result = (
    ticket_rows
    .unionByName(event_rows)
    .distinct()
    .orderBy("entity_id")
)
