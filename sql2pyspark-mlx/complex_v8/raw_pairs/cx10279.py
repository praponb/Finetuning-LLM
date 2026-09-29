from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .select(
        F.lit("event").alias("origin"),
        F.col("id").alias("key_id"),
        F.col("event_type").alias("category"),
        "event_at",
    )
)

event_side = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        F.lit("event_2").alias("origin"),
        F.col("id").alias("key_id"),
        F.col("event_type").alias("category"),
        "event_at",
    )
)

result = event_rows.unionByName(event_side)
