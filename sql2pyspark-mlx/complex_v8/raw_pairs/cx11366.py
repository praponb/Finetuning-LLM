from pyspark.sql import functions as F

ticket_side = df_tickets.select("requester_id", F.col("channel").alias("label"))

ticket_rows = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("closed_at") >= F.lit("2025-01-20 22:00:00").cast("timestamp")))
    .select(F.col("csat").alias("requester_id"), F.col("priority").alias("label"))
)

result = (
    ticket_side
    .intersect(ticket_rows)
    .orderBy(F.col("requester_id").desc())
)
