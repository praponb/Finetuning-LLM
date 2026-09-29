from pyspark.sql import functions as F

resolved_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("resolved_count"))
)

escalated_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("escalated_count"))
)

result = (
    resolved_monthly.alias("a")
    .join(escalated_monthly.alias("b"), F.col("a.event_at_month") == F.col("b.event_at_month"), "full")
    .select(
        F.coalesce(F.col("a.event_at_month"), F.col("b.event_at_month")).alias("event_at_month"),
        F.coalesce(F.col("a.resolved_count"), F.lit(0)).alias("resolved_count"),
        F.coalesce(F.col("b.escalated_count"), F.lit(0)).alias("escalated_count"),
        (F.coalesce(F.col("a.resolved_count"), F.lit(0)) - F.coalesce(F.col("b.escalated_count"), F.lit(0))).alias("resolved_count_minus_escalated_count"),
    )
    .orderBy("event_at_month")
)
