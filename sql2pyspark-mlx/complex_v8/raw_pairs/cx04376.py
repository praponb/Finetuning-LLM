from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.col("ticket_id").isNotNull()))
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
    .filter((F.col("event_type").isin("created", "assigned", "reopened")) & (F.col("ticket_id").isNotNull()))
    .select(
        F.col("id").alias("ref_id"),
        "event_at",
        "actor",
        F.lit("event").alias("record_type"),
    )
)

event_side_2 = (
    df_ticket_events
    .select(
        "id",
        "event_at",
        "event_type",
        F.lit("event_2").alias("record_type"),
    )
)

result = (
    event_side
    .union(event_side_2)
    .orderBy("event_at")
)
