from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("channel").isin("chat", "phone", "email")))
    .select("requester_id", F.col("channel").alias("category"))
)

event_side = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("actor") != "customer"))
    .select(
        F.col("ticket_id").alias("requester_id"),
        F.col("actor").alias("category"),
    )
)

result = (
    ticket_side
    .intersectAll(event_side)
    .orderBy("category")
)
