from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("priority") != "P1")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 2, "medium").otherwise("small").alias("csat_tier"),
    )
)

tagged = (
    cleaned
    .filter(F.col("days_since_closed_at") > 280)
    .select(
        "*",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.coalesce(F.col("csat_tier"), F.lit("UNKNOWN")).alias("csat_tier_or_unknown"),
    )
)

csat_tier_or_unknown_stats = (
    tagged
    .groupBy("csat_tier_or_unknown")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

vs_group = (
    tagged.alias("s")
    .join(csat_tier_or_unknown_stats.alias("g"), F.col("g.csat_tier_or_unknown") == F.col("s.csat_tier_or_unknown"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

line_level_events = (
    vs_group.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.csat_tier",
        "s.channel_priority_label",
        "s.csat_tier_or_unknown",
        "s.requester_id_minus_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

filtered = (
    line_level_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "created") & (F.col("t.event_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")), "left_semi")
)

result = (
    filtered
    .filter(F.col("requester_id_minus_avg").between(-10.0, 12.5))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat_tier",
        "channel_priority_label",
        "csat_tier_or_unknown",
        "requester_id_minus_avg",
        "event_id",
        "event_actor",
        "event_type",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_event_at"),
        F.coalesce(F.col("csat_tier"), F.lit("UNKNOWN")).alias("csat_tier_or_unknown_2"),
    )
    .orderBy(F.col("csat_tier").desc(), F.col("csat_tier_or_unknown_2").desc())
)
