from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("event_type") != "reopened")
    .select("id", "ticket_id", F.lit("event").alias("source"))
)

event_rows = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) & (F.col("actor") == "agent"))
    .select("id", "ticket_id", F.lit("event_2").alias("source"))
)

stacked = event_side.union(event_rows)

result = (
    stacked
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ticket_id").alias("total_ticket_id"),
    )
    .orderBy("source")
)
