from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")) | (F.size(F.col("tags")) >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

without_matches = (
    recent
    .crossJoin(recent.agg(F.min("requester_id").alias("overall_min_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_min_requester_id"))
    .drop("overall_min_requester_id")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_closed_at", "t.first_ticket_opened_at")
)

with_activity = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "closed_at_date",
        "priority_or_unknown",
        "last_ticket_closed_at",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
    .orderBy("requester_id")
)
