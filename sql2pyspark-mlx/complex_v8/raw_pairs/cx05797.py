from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
    )
)

with_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.tags",
        F.col("su.team").alias("agent_team"),
    )
)

derived = (
    with_details_agent
    .filter(F.col("channel").isin("chat", "phone"))
    .select(
        "id",
        "agent_id",
        "channel",
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .rollup("agent_tier", "agent_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("channel").isNotNull(), 1)) / F.count("*"), 1).alias("pct_channel_is_not_null"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
