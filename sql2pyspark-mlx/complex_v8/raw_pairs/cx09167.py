from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        F.col("ticket_id").alias("ref_id"),
        F.col("event_at").alias("happened_at"),
        F.lit("event").alias("record_type"),
    )
)

ticket_side = (
    df_tickets
    .filter((F.col("csat") >= 1) & (F.col("priority").isin("P3", "P2")))
    .select(
        F.col("agent_id").alias("ref_id"),
        F.col("closed_at").alias("happened_at"),
        F.lit("ticket").alias("record_type"),
    )
)

result = (
    event_side
    .unionByName(ticket_side)
    .orderBy("happened_at")
)
