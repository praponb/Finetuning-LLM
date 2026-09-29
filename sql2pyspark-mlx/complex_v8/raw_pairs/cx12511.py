from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.when(F.col("channel").isin("chat", "email"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

joined_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.channel_group",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

annotated = (
    joined_agent
    .select(
        "id",
        "channel",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

result = (
    annotated
    .filter(F.col("channel_slug").isNotNull())
    .select(
        "id",
        "channel_slug",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
    .orderBy("channel_ref", "id")
    .limit(5)
)
