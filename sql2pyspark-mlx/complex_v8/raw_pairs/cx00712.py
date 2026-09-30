from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")) & (F.month(F.col("opened_at")).isin(1, 2)))
    .select("id")
)

ticket_rows = (
    df_tickets
    .filter((F.col("agent_id") >= 6) & (F.col("closed_at") < F.lit("2025-02-15 09:00:00").cast("timestamp")))
    .select("id")
)

result = ticket_side.subtract(ticket_rows)
