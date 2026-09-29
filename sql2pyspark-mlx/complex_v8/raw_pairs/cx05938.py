from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) | (F.col("csat") < 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

filtered = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") != "system"), "left_anti")
    .filter(F.array_contains(F.col("tags"), "bug"))
)

channel_stats = (
    filtered
    .groupBy("channel")
    .agg(F.max("csat").alias("max_csat"))
)

compared = (
    filtered.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

result = (
    compared
    .cube("agent_id", "channel", "priority")
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.round(F.sum("max_csat") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("max_csat_to_csat_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "is_total_agent_id",
        "max_csat_to_csat_ratio",
        "distinct_ids",
    )
)
