from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 9)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

per_group = (
    filtered
    .groupBy("actor", "ticket_id")
    .agg(
        F.max("days_since_event_at").alias("max_days_since_event_at"),
        F.sum(F.when(F.col("event_type").isin("reopened", "replied"), F.col("days_since_event_at"))).alias("days_since_event_at_event_type_in_reopened_repli"),
        F.round(F.sum("event_at_hour") / F.nullif(F.sum("days_since_event_at"), F.lit(0)), 3).alias("event_at_hour_to_days_since_event_at_ratio"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.round(F.avg("event_at_hour"), 2).alias("avg_event_at_hour"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    per_group.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

qualified = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "assigned"), "left_anti")
)

line_level_events = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(7, 9)), "left")
    .select(
        "s.actor",
        "s.ticket_id",
        "s.max_days_since_event_at",
        "s.days_since_event_at_event_type_in_reopened_repli",
        "s.distinct_event_types",
        "s.distinct_ticket_ids",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    line_level_events
    .select(
        "actor",
        "ticket_id",
        "max_days_since_event_at",
        "distinct_event_types",
        "distinct_ticket_ids",
        "event_id",
        "event_actor",
        "event_at",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
    .orderBy(F.col("distinct_ticket_ids").desc(), F.col("event_at_label").asc())
)
