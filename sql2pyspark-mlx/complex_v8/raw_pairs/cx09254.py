from pyspark.sql import functions as F

ticket_rows = df_tickets.select("id")

ticket_side = df_tickets.select("id")

result = (
    ticket_rows
    .exceptAll(ticket_side)
    .orderBy(F.col("id").desc())
)
