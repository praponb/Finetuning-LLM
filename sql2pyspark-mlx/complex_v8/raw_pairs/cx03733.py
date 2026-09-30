from pyspark.sql import functions as F

replied_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("replied_count"))
)

resolved_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("resolved_count"))
)

result = (
    replied_monthly.alias("a")
    .join(resolved_monthly.alias("b"), F.col("a.event_at_month") == F.col("b.event_at_month"), "full")
    .select(
        F.coalesce(F.col("a.event_at_month"), F.col("b.event_at_month")).alias("event_at_month"),
        F.coalesce(F.col("a.replied_count"), F.lit(0)).alias("replied_count"),
        F.coalesce(F.col("b.resolved_count"), F.lit(0)).alias("resolved_count"),
        (F.coalesce(F.col("a.replied_count"), F.lit(0)) - F.coalesce(F.col("b.resolved_count"), F.lit(0))).alias("replied_count_minus_resolved_count"),
    )
    .orderBy("event_at_month")
)
