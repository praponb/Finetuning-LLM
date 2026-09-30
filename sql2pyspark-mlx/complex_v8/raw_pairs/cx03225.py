from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("id").alias("qty"))

ticket_side_2 = df_tickets.select("id")

result = (
    ticket_side
    .intersect(ticket_side_2)
    .orderBy(F.col("qty").desc())
)
