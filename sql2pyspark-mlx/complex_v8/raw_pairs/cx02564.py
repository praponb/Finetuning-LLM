from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

filtered = (
    recent_tickets
    .crossJoin(recent_tickets.agg(F.avg("channel_len").alias("overall_avg_channel_len")))
    .filter(F.col("channel_len") > F.col("overall_avg_channel_len"))
    .drop("overall_avg_channel_len")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .groupBy("channel_len")
    .pivot("channel", ["email", "web", "phone", "chat"])
    .agg(F.min("event_count"))
    .orderBy("channel_len")
)
