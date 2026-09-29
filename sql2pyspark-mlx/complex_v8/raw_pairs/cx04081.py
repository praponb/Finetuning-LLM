from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(
        F.lit("event").alias("record_type"),
        F.col("ticket_id").alias("qty"),
        F.col("event_at").alias("event_time"),
    )
)

event_side_2 = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_at") < F.lit("2025-02-12 15:00:00").cast("timestamp")))
    .select(F.lit("event_2").alias("record_type"), "ticket_id", "event_at")
)

all_events = event_side.union(event_side_2)

result = (
    all_events
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("record_type")
)
