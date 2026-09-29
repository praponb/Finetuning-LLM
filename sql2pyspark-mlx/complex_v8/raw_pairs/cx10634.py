from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .select(
        F.col("ticket_id").alias("qty"),
        "event_at",
        F.col("event_type").alias("category"),
        F.lit("event").alias("src"),
    )
)

event_rows = (
    df_ticket_events
    .select(
        "ticket_id",
        "event_at",
        "event_type",
        F.lit("event_2").alias("src"),
    )
)

result = event_side.union(event_rows)
