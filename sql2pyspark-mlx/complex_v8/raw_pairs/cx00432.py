from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("priority") == "P2") & (F.col("channel") != "chat"))
    .select("id")
)

ticket_side = df_tickets.select(F.col("agent_id").alias("id"))

result = (
    ticket_rows
    .subtract(ticket_side)
    .orderBy(F.col("id").desc())
)
