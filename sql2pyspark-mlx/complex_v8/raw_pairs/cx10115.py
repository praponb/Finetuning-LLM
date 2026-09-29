from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .select(
        F.col("id").alias("entity_id"),
        F.col("agent_id").alias("entity_id_2"),
        F.col("opened_at").alias("happened_at"),
    )
)

ticket_rows = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select("csat", "requester_id", "opened_at")
)

result = (
    ticket_side
    .union(ticket_rows)
    .distinct()
    .orderBy(F.col("entity_id").desc(), "happened_at")
)
