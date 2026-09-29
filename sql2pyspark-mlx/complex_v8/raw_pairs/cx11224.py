from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

exploded_detail_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("agent", "customer")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.days_since_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

stats = (
    exploded_detail_events
    .groupBy(
        F.to_date("event_event_at").alias("event_event_at_day"),
        F.date_trunc("month", "event_at").alias("event_at_month"),
    )
    .agg(
        F.min("days_since_event_at").alias("min_days_since_event_at"),
        F.round(F.avg("days_since_event_at"), 2).alias("avg_days_since_event_at"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("event_event_type").isin("resolved", "assigned", "reopened"), 1).otherwise(0)).alias("n_event_event_type_in_resolved"),
        F.round(100 * F.count(F.when(F.col("event_actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_actor_is_not_null"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    stats
    .filter(F.col("min_days_since_event_at") <= 560)
    .select(
        "event_event_at_day",
        "row_count",
        "n_event_event_type_in_resolved",
        (F.floor(F.col("avg_days_since_event_at") / 100) * 100).alias("avg_days_since_event_at_bucket"),
    )
    .orderBy(
        F.col("row_count").desc(),
        F.col("avg_days_since_event_at_bucket").asc_nulls_last(),
    )
)
