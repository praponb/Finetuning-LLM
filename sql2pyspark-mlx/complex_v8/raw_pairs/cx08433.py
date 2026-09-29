from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.col("channel") != "phone")
    .select("agent_id")
)

ticket_rows_2 = (
    df_tickets
    .filter(F.col("requester_id") <= 19)
    .select("agent_id")
)

result = (
    ticket_rows
    .intersect(ticket_rows_2)
    .orderBy("agent_id")
)
