from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("channel").isNotNull()) & (F.col("requester_id") <= 18))
    .select(F.col("csat").alias("key_id"), F.col("requester_id").alias("entity_id"))
)

ticket_side = (
    df_tickets
    .select(
        F.col("agent_id").alias("key_id"),
        F.col("requester_id").alias("entity_id"),
    )
)

result = ticket_rows.unionByName(ticket_side)
