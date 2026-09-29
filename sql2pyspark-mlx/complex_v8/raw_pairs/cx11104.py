from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    valid
    .filter(F.col("event_type") == "escalated")
    .select(
        "event_type",
        F.col("event_at").alias("event_time"),
        F.lit("event").alias("source"),
    )
)

event_side_2 = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 15)) & (F.col("ticket_id").isNotNull()))
    .select("actor", "event_at", F.lit("event_2").alias("source"))
)

all_events = (
    event_side
    .union(event_side_2)
    .distinct()
)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
