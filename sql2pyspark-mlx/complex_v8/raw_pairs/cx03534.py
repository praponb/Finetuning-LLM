from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "refund"))
    .select("channel")
)

ticket_rows = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(F.col("priority").alias("channel"))
)

result = ticket_side.subtract(ticket_rows)
