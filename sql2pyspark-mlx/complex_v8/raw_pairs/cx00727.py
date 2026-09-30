from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (~F.col("event_type").isin("assigned", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    scoped_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")) & (F.col("actor") == "agent"))
    .select("actor", "event_type", F.col("event_at").alias("happened_at"))
)

event_side_2 = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select("event_type", "actor", "event_at")
)

result = (
    event_side
    .union(event_side_2)
    .distinct()
)
