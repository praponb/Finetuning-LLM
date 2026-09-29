from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.col("channel") == "phone") & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

annotated = (
    base
    .select(
        "id",
        "requester_id",
        "priority",
        "channel",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    annotated.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

scored = (
    with_counts
    .filter(F.col("requester_id_per_csat") > 4.0)
    .select(
        "*",
        F.abs(F.col("requester_id_per_csat") - 5.0).alias("requester_id_per_csat_distance"),
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "priority",
        "channel",
        "requester_id_per_csat",
        "event_count",
        "requester_id_per_csat_distance",
        "channel_priority_label",
    )
    .orderBy(F.col("id").desc())
)
