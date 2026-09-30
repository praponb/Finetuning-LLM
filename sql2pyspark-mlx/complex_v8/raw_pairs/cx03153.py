from pyspark.sql import functions as F

replied_by_ticket = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .groupBy("ticket_id")
    .agg(F.count("*").alias("replied_count"))
)

escalated_by_ticket = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .groupBy("ticket_id")
    .agg(F.count("*").alias("escalated_count"))
)

result = (
    replied_by_ticket.alias("a")
    .join(escalated_by_ticket.alias("b"), F.col("a.ticket_id") == F.col("b.ticket_id"), "full")
    .select(
        F.coalesce(F.col("a.ticket_id"), F.col("b.ticket_id")).alias("ticket_id"),
        F.coalesce(F.col("a.replied_count"), F.lit(0)).alias("replied_count"),
        F.coalesce(F.col("b.escalated_count"), F.lit(0)).alias("escalated_count"),
        (F.coalesce(F.col("a.replied_count"), F.lit(0)) - F.coalesce(F.col("b.escalated_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("ticket_id")
)
