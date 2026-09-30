from pyspark.sql import functions as F

filtered_events = df_ticket_events

event_side = (
    filtered_events
    .filter((F.hour(F.col("event_at")).between(7, 11)) & (F.col("ticket_id").isNotNull()))
    .select(F.col("id").alias("ref_id"), "ticket_id", F.lit("event").alias("source"))
)

event_rows = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.hour(F.col("event_at")).between(8, 11)))
    .select("id", "ticket_id", F.lit("event_2").alias("source"))
)

combined = event_side.union(event_rows)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ticket_id").alias("total_ticket_id"),
    )
    .orderBy("source")
)
