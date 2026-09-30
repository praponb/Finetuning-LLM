from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("channel").isNotNull()) & (F.col("closed_at") < F.lit("2025-02-10 04:00:00").cast("timestamp")))
    .select("csat", "requester_id")
)

ticket_side_2 = df_tickets.select("requester_id", "agent_id")

result = ticket_side.union(ticket_side_2)
