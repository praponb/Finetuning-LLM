from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("priority") != "P2") & (F.col("channel").isin("chat", "email")))
    .select(F.col("id").alias("key_id"))
)

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select("id")
)

result = ticket_side.subtract(event_rows)
