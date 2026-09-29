from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .select(
        F.col("requester_id").alias("qty"),
        "agent_id",
        "closed_at",
        F.lit("ticket").alias("record_type"),
    )
)

event_side = (
    df_ticket_events
    .select(
        "ticket_id",
        "id",
        "event_at",
        F.lit("event").alias("record_type"),
    )
)

all_events = ticket_side.union(event_side)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("agent_id").alias("total_agent_id"),
        F.max("closed_at").alias("latest_closed_at"),
    )
    .orderBy("record_type")
)
