from pyspark.sql import functions as F

resolved_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("resolved_count"))
)

reopened_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("reopened_count"))
)

result = (
    resolved_monthly.alias("a")
    .join(reopened_monthly.alias("b"), F.col("a.event_at_month") == F.col("b.event_at_month"), "full")
    .select(
        F.coalesce(F.col("a.event_at_month"), F.col("b.event_at_month")).alias("event_at_month"),
        F.coalesce(F.col("a.resolved_count"), F.lit(0)).alias("resolved_count"),
        F.coalesce(F.col("b.reopened_count"), F.lit(0)).alias("reopened_count"),
        (F.coalesce(F.col("a.resolved_count"), F.lit(0)) - F.coalesce(F.col("b.reopened_count"), F.lit(0))).alias("resolved_count_minus_reopened_count"),
    )
    .orderBy("event_at_month")
)
