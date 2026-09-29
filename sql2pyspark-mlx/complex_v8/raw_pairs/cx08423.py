from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((~F.array_contains(F.col("tags"), "refund")) & (F.col("csat").between(2, 4)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

channel_or_unknown_stats = (
    base
    .groupBy("channel_or_unknown")
    .agg(F.max("csat").alias("max_csat"))
)

outliers = (
    base.alias("s")
    .join(channel_or_unknown_stats.alias("g"), F.col("g.channel_or_unknown") == F.col("s.channel_or_unknown"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

combined_details = (
    outliers.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.hours_closed_at_to_opened_at",
        "s.max_csat",
        "s.csat_minus_max",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .groupBy("priority", "agent_id")
    .pivot("agent_team", ["Tech", "VIP"])
    .agg(F.min("csat").alias("min_v"), F.count("csat").alias("n"))
)
