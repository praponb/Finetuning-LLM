from pyspark.sql import functions as F

escalated_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("escalated_count"))
)

resolved_monthly = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .groupBy(F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(F.count("*").alias("resolved_count"))
)

result = (
    escalated_monthly.alias("a")
    .join(resolved_monthly.alias("b"), F.col("a.event_at_month") == F.col("b.event_at_month"), "full")
    .select(
        F.coalesce(F.col("a.event_at_month"), F.col("b.event_at_month")).alias("event_at_month"),
        F.coalesce(F.col("a.escalated_count"), F.lit(0)).alias("escalated_count"),
        F.coalesce(F.col("b.resolved_count"), F.lit(0)).alias("resolved_count"),
        (F.coalesce(F.col("a.escalated_count"), F.lit(0)) - F.coalesce(F.col("b.resolved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("event_at_month")
)
