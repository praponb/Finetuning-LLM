from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date")))
    .select(
        F.lit("event").alias("record_type"),
        F.col("event_type").alias("category"),
        F.col("event_at").alias("event_time"),
        F.col("actor").alias("category_2"),
    )
)

event_side_2 = (
    df_ticket_events
    .select(
        F.lit("event_2").alias("record_type"),
        "event_type",
        "event_at",
        "actor",
    )
)

ticket_side = (
    df_tickets
    .select(
        F.lit("ticket").alias("record_type"),
        "priority",
        "closed_at",
        "channel",
    )
)

result = (
    event_side
    .union(event_side_2)
    .union(ticket_side)
)
