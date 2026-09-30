from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 7) & (F.col("closed_at") < F.lit("2025-02-14 22:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
    )
)

joined_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.channel_ref",
        "su.agent_name",
    )
)

tagged = (
    joined_agent
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "channel",
        "channel_ref",
        "agent_name",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
        F.lower(F.regexp_replace(F.trim(F.col("channel_ref")), " ", "_")).alias("channel_ref_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        "channel",
        "agent_name",
        "channel_ref_slug",
        "channel_slug",
        F.concat(F.col("channel_slug"), F.lit("#"), F.col("id").cast("string")).alias("channel_slug_ref"),
        F.concat_ws("|", F.col("agent_name"), F.col("channel_ref")).alias("agent_name_channel_ref_label"),
    )
    .orderBy("channel_ref_slug", "id")
)
