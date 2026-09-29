from pyspark.sql import functions as F

ticket_rows = df_tickets.select(F.col("id").alias("qty"))

ticket_side = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select("agent_id")
)

result = (
    ticket_rows
    .subtract(ticket_side)
    .orderBy("qty")
)
