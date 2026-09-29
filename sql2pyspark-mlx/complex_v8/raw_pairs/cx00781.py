from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("priority").isin("P4", "P2")))
    .select("id")
)

ticket_side_2 = (
    df_tickets
    .filter((F.col("agent_id") >= 2) & (F.col("csat").between(1, 4)))
    .select("id")
)

result = (
    ticket_side
    .intersectAll(ticket_side_2)
    .orderBy("id")
)
