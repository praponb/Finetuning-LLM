from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.abs(F.col("requester_id") - 8).alias("requester_id_distance"),
    )
)

labeled = (
    cleaned_tickets
    .select(
        "*",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled_2"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id_distance").cast("string")).alias("channel_ref"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel") == "email", 1)).alias("ticket_channel_count"),
        F.count(F.when(F.col("priority") != "P4", 1)).alias("ticket_priority_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.last_ticket_closed_at",
    )
)

result = (
    with_stats
    .rollup("ticket_priority_count", "agent_id")
    .agg(
        F.round(F.avg("ticket_channel_count"), 2).alias("avg_ticket_channel_count"),
        F.count(F.when(F.col("channel").isin("web", "email"), 1)).alias("n_channel_in_web_email"),
    )
    .orderBy(
        F.col("ticket_priority_count").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
