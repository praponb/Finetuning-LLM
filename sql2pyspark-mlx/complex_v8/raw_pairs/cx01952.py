from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "assigned", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

derived = (
    base_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("event_type").isin("resolved", "replied"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_counts = (
    derived.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_counts
    .filter(F.col("event_type_prefix").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_type_group",
        "event_type_prefix",
        "last_event_at",
        "event_count",
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("last_event_at").desc(), "id")
    .limit(20)
)
