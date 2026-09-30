from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date")) & (F.col("ticket_id") >= 46))
    .select(
        F.lit("event").alias("origin"),
        "actor",
        "event_at",
        "event_type",
    )
)

event_side = (
    df_ticket_events
    .select(
        F.lit("event_2").alias("origin"),
        "actor",
        "event_at",
        "event_type",
    )
)

result = (
    event_rows
    .unionByName(event_side)
    .distinct()
)
