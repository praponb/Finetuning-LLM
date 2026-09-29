from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("requester_id") < 14)
    .select(F.col("csat").alias("ref_id"))
)

ticket_rows = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp")) & (F.col("csat") > 2))
    .select("id")
)

result = ticket_side.subtract(ticket_rows)
