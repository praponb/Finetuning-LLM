from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("channel").isNotNull())
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
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

annotated = (
    valid
    .select(
        "*",
        (F.col("opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("opened_at_plus_15m"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

priority_stats = (
    annotated
    .groupBy("priority")
    .agg(F.max("csat").alias("max_csat"))
)

vs_group = (
    annotated.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

enriched = (
    vs_group
    .filter(F.hour(F.col("opened_at_plus_15m")).between(10, 13))
    .select(
        "id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "max_csat",
        "csat_minus_max",
        F.round(F.col("csat_minus_max") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_minus_max_per_requester_id"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
        F.coalesce(F.col("priority_len"), F.lit(0)).alias("priority_len_filled"),
    )
)

enriched_2 = (
    enriched
    .select(
        "id",
        "priority",
        "tags",
        "max_csat",
        "csat_minus_max",
        "csat_minus_max_per_requester_id",
        "priority_len_filled",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

line_level_events = (
    enriched_2.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.priority",
        "s.tags",
        "s.priority_len_filled",
        "s.priority_slug",
        "s.opened_at_label",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    line_level_events
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "priority_len_filled",
        "priority_slug",
        "opened_at_label",
        "event_at",
        "event_actor",
        F.month(F.col("event_at")).alias("event_at_month"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy("event_at", F.col("priority_len_filled").asc())
)
