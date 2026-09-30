from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select("ticket_id", F.col("id").alias("entity_id"), F.lit("event").alias("src"))
)

ticket_side = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select("csat", "id", F.lit("ticket").alias("src"))
)

result = (
    event_side
    .union(ticket_side)
    .orderBy(F.col("ticket_id").desc())
)
