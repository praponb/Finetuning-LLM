from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter((F.col("requester_id") >= 3) & (F.col("agent_id") <= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.abs(F.col("requester_id") - 12).alias("requester_id_distance"),
    )
)

totals = (
    scoped
    .groupBy("priority_or_unknown", "agent_id", "channel")
    .agg(
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_to_requester_id_ratio"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.min("closed_at").alias("first_closed_at"),
    )
)

with_details = (
    totals.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-02-26").cast("date"))), "inner")
    .select("s.*", "su.agent_name", F.col("su.tier").alias("agent_tier"))
)

with_parent = (
    with_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.priority_or_unknown",
        "s.agent_id",
        "s.channel",
        "s.csat_to_requester_id_ratio",
        "s.distinct_agent_ids",
        "s.first_closed_at",
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    with_parent
    .select(
        "priority_or_unknown",
        "csat_to_requester_id_ratio",
        "distinct_agent_ids",
        "first_closed_at",
        "agent_agent_name",
        F.substring(F.trim(F.col("agent_agent_name")), 1, 2).alias("agent_agent_name_prefix"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)
