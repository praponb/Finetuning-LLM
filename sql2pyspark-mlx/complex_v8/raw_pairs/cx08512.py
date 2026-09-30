from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

derived = (
    filtered_events
    .filter(F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_date",
        "actor_clean",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_stats
    .groupBy("ticket_id")
    .pivot("event_type", ["escalated", "created", "reopened"])
    .agg(F.min("event_count").alias("min_v"), F.max("event_count").alias("max_v"))
    .orderBy("ticket_id")
)
