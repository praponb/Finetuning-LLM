from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("id").alias("key_id"), F.col("csat").alias("qty"))

ticket_rows = (
    df_tickets
    .filter(F.col("requester_id").between(5, 19))
    .select("id", "csat")
)

event_side = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select("id", "ticket_id")
)

combined = (
    ticket_side
    .union(ticket_rows)
    .union(event_side)
)

result = (
    combined
    .groupBy("key_id")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("key_id")
)
