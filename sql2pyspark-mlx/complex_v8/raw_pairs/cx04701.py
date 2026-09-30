from pyspark.sql import functions as F

reopened_by_ticket = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .groupBy("ticket_id")
    .agg(F.count("*").alias("reopened_count"))
)

resolved_by_ticket = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .groupBy("ticket_id")
    .agg(F.count("*").alias("resolved_count"))
)

result = (
    reopened_by_ticket.alias("a")
    .join(resolved_by_ticket.alias("b"), F.col("a.ticket_id") == F.col("b.ticket_id"), "full")
    .select(
        F.coalesce(F.col("a.ticket_id"), F.col("b.ticket_id")).alias("ticket_id"),
        F.coalesce(F.col("a.reopened_count"), F.lit(0)).alias("reopened_count"),
        F.coalesce(F.col("b.resolved_count"), F.lit(0)).alias("resolved_count"),
        (F.coalesce(F.col("a.reopened_count"), F.lit(0)) - F.coalesce(F.col("b.resolved_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("ticket_id")
)
