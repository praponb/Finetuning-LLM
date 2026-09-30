from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

enriched = (
    prepared_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "csat",
        "requester_id_bucket",
        "channel_priority_label",
        "channel_clean",
        F.upper(F.trim(F.col("channel_priority_label"))).alias("channel_priority_label_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        "t.first_event_at_2",
    )
)

enriched_2 = (
    with_counts
    .select(
        "*",
        F.greatest(F.col("csat"), F.col("requester_id_bucket")).alias("greatest_csat_requester_id_bucket"),
        F.greatest(F.col("requester_id_bucket"), F.col("requester_id")).alias("greatest_requester_id_bucket_requester_id"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

result = (
    enriched_2
    .groupingSets(
        [["priority_or_unknown", "priority", "csat"], ["priority_or_unknown"], []],
        "priority_or_unknown", "priority", "csat",
    )
    .agg(
        F.sum(F.when(F.col("priority") == "P2", 1).otherwise(0)).alias("n_priority_p2"),
        F.sum("greatest_csat_requester_id_bucket").alias("total_greatest_csat_requester_id_bucket"),
    )
)
