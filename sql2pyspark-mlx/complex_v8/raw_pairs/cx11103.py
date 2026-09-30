from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .select(
        F.lit("event").alias("src"),
        F.col("ticket_id").alias("ref_id"),
        F.col("id").alias("key_id"),
        "event_at",
    )
)

ticket_side = (
    df_tickets
    .select(
        F.lit("ticket").alias("src"),
        F.col("id").alias("ref_id"),
        F.col("agent_id").alias("key_id"),
        F.col("closed_at").alias("event_at"),
    )
)

combined = (
    event_rows
    .unionByName(ticket_side)
    .distinct()
)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("event_at").alias("latest_event_at"),
    )
    .orderBy("src")
)
