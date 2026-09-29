from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("csat") < 3))
    .select(
        F.lit("ticket").alias("src"),
        F.col("agent_id").alias("qty"),
        F.col("priority").alias("name"),
    )
)

event_side = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        F.lit("event").alias("src"),
        F.col("ticket_id").alias("qty"),
        F.col("actor").alias("name"),
    )
)

result = (
    ticket_side
    .unionByName(event_side)
    .distinct()
    .orderBy("name")
)
