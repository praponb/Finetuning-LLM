from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.col("requester_id").between(7, 17)) & (F.col("csat") < 4))
    .select("csat", "closed_at", F.lit("ticket").alias("origin"))
)

event_side = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 10))
    .select(
        F.col("id").alias("csat"),
        F.col("event_at").alias("closed_at"),
        F.lit("event").alias("origin"),
    )
)

all_events = ticket_side.unionByName(event_side)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("csat").alias("total_csat"),
        F.max("closed_at").alias("latest_closed_at"),
    )
    .orderBy("origin")
)
