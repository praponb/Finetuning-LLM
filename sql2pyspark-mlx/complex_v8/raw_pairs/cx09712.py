from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("requester_id").between(5, 22))
    .select("requester_id", F.col("csat").alias("key_id"))
)

event_side = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 14)) & (F.col("event_type").isNotNull()))
    .select(F.col("id").alias("requester_id"), F.col("ticket_id").alias("key_id"))
)

stacked = (
    ticket_side
    .union(event_side)
    .distinct()
)

result = (
    stacked
    .groupBy("requester_id")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("requester_id")
)
