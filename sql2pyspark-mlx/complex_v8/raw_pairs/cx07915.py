from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-03-04 05:00:00").cast("timestamp")) & (F.size(F.col("tags")) >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.sum("requester_id").alias("total_ticket_requester_id"),
    )
)

with_stats = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.total_ticket_requester_id",
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.closed_at_year",
        "s.channel_len",
        "s.priority_or_unknown",
        "s.ticket_count",
        "s.total_ticket_requester_id",
        "su.agent_name",
    )
)

without_matches = (
    with_parent
    .crossJoin(with_parent.agg(F.avg("requester_id").alias("overall_avg_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_avg_requester_id"))
    .drop("overall_avg_requester_id")
)

result = (
    without_matches
    .groupingSets(
        [["channel_len", "priority", "ticket_count"], ["channel_len"], ["priority"]],
        "channel_len", "priority", "ticket_count",
    )
    .agg(
        F.sum("total_ticket_requester_id").alias("total_total_ticket_requester_id"),
        F.sum("requester_id").alias("total_requester_id"),
    )
    .orderBy(
        F.col("channel_len").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
        F.col("ticket_count").asc_nulls_last(),
    )
)
