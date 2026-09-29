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

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type").isin("resolved", "escalated"), 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
    )
)

result = (
    with_stats
    .rollup("actor", "event_type", "ticket_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_event_type_count").alias("min_event_event_type_count"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "cnt",
        "min_event_event_type_count",
    )
)
