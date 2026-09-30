from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    scoped_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_stats
    .groupBy("ticket_count")
    .pivot("priority", ["P1", "P2", "P4", "P3"])
    .agg(F.sum("csat").alias("total"), F.count("csat").alias("n"))
    .orderBy("ticket_count")
)
