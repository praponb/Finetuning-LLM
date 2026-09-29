from pyspark.sql import functions as F

ticket_rows = df_tickets.select("csat", "priority")

ticket_rows_2 = (
    df_tickets
    .filter((F.col("agent_id") >= 6) & (F.col("priority") == "P2"))
    .select("id", "channel")
)

result = ticket_rows.intersect(ticket_rows_2)
