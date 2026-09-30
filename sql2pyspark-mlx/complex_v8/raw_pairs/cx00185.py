from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    base
    .filter((F.col("event_type") == "assigned") & (F.hour(F.col("event_at")).between(9, 15)))
    .select("event_type", F.col("actor").alias("category"))
)

event_rows = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(9, 15)) & (F.col("event_type") == "created"))
    .select("event_type", "actor")
)

result = (
    event_side
    .union(event_rows)
    .orderBy(F.col("category").desc(), "event_type")
)
