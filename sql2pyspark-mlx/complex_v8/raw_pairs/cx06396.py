from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .groupBy("ticket_id", "event_at")
    .pivot("event_type", ["assigned", "reopened", "escalated", "created"])
    .agg(F.count("event_count"))
    .orderBy("ticket_id", "event_at")
)
