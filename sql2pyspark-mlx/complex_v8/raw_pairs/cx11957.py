from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.col("csat") >= 3)
    .select(F.col("agent_id").alias("qty"), "channel")
)

ticket_rows = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(9, 14))
    .select(F.col("requester_id").alias("qty"), F.col("priority").alias("channel"))
)

result = (
    ticket_side
    .exceptAll(ticket_rows)
    .orderBy(F.col("channel").desc())
)
