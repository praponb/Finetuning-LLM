from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("channel").alias("category"))

ticket_side_2 = (
    df_tickets
    .filter((F.col("priority").isin("P1", "P4")) & (F.col("channel").isin("web", "chat")))
    .select("priority")
)

result = (
    ticket_side
    .intersect(ticket_side_2)
    .orderBy(F.col("category").desc())
)
