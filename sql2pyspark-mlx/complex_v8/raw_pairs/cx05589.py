from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    cleaned
    .filter((F.col("event_type") == "assigned") & (F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date")))
    .select(
        F.lit("event").alias("origin"),
        F.col("ticket_id").alias("qty"),
        F.col("id").alias("key_id"),
    )
)

event_rows = (
    df_ticket_events
    .select(
        F.lit("event_2").alias("origin"),
        F.col("ticket_id").alias("qty"),
        F.col("id").alias("key_id"),
    )
)

stacked = event_side.union(event_rows)

result = (
    stacked
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("origin")
)
