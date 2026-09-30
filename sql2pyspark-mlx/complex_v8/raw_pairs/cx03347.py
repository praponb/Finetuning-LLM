from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .select(
        F.col("priority").alias("label"),
        F.col("closed_at").alias("event_time"),
        F.col("id").alias("entity_id"),
        F.lit("ticket").alias("origin"),
    )
)

ticket_side = (
    df_tickets
    .select(
        "channel",
        "opened_at",
        "agent_id",
        F.lit("ticket_2").alias("origin"),
    )
)

result = (
    ticket_rows
    .union(ticket_side)
    .orderBy("origin", "label")
)
