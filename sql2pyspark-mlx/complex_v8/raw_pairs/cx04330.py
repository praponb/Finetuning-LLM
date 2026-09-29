from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

with_children_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 14)), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

profiled = (
    with_children_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_id",
        "event_event_at",
        "event_event_type",
        "event_count",
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
    )
    .orderBy(F.col("event_event_at").desc_nulls_first())
)
