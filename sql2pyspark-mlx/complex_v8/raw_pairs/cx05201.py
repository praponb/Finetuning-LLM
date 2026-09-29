from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("agent_id") >= 9)
    .select(F.col("priority").alias("label"), "closed_at")
)

ticket_side_2 = (
    df_tickets
    .filter(F.col("csat") > 3)
    .select("channel", "closed_at")
)

result = (
    ticket_side
    .union(ticket_side_2)
    .orderBy("closed_at")
)
