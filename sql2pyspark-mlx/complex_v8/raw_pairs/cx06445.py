from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 15:00:00").cast("timestamp")) & (F.col("ticket_id") <= 27))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    scoped
    .withColumn("days_since_event_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")))
)

event_type_stats = (
    enriched
    .groupBy("event_type")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

above_average = (
    enriched.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("actor").isin("customer", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "days_since_event_at",
        "median_days_since_event_at",
        "days_since_event_at_minus_median",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
    .orderBy(F.col("event_type_ref").desc(), F.col("event_at").desc(), "id")
    .limit(5)
)
