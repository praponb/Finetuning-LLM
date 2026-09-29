from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

enriched = (
    base
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel").isin("phone", "chat", "web"), 1)).alias("ticket_channel_count"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

result = (
    with_stats
    .cube("agent_id", "csat", "priority")
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.round(F.sum("ticket_channel_count") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("ticket_channel_count_to_csat_ratio"),
    )
    .select(
        "agent_id",
        "csat",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "is_total_agent_id",
        "ticket_channel_count_to_csat_ratio",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
