from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .select(
        F.lit("event").alias("origin"),
        F.col("id").alias("ref_id"),
        "event_at",
        F.col("event_type").alias("label"),
    )
)

event_rows_2 = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        F.lit("event_2").alias("origin"),
        "id",
        "event_at",
        "event_type",
    )
)

all_events = event_rows.union(event_rows_2)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_at").alias("latest_event_at"),
    )
    .orderBy("origin")
)
