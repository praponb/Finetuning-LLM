from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

enriched = (
    with_counts
    .filter(F.col("event_count") < 7)
    .select(
        "*",
        F.when(F.col("requester_id") >= 20, "A").when(F.col("requester_id") >= 5, "B").otherwise("C").alias("requester_id_tier"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-12").cast("date")) & (F.col("t.requester_id") >= 5), "left_semi")
)

result = (
    without_matches
    .cube("priority", "tags_count", "requester_id_tier")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("tags_count").alias("total_tags_count"),
        F.sum(F.when(F.col("priority") == "P2", F.col("requester_id")).otherwise(0)).alias("requester_id_priority_p2"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "tags_count",
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        "gid",
        "total_tags_count",
        "requester_id_priority_p2",
        "priority_list",
    )
)
