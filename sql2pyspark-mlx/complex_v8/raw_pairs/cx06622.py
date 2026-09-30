from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(8, 12)) & (F.col("agent_id") >= 8))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 18, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
        F.when(F.col("channel").isin("web", "email"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_details_agent = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.greatest_requester_id_csat",
        "s.channel_group",
        F.col("su.tier").alias("agent_tier"),
    )
)

scored = (
    with_details_agent
    .withColumn("channel_or_unknown", F.coalesce(F.col("channel"), F.lit("UNKNOWN")))
)

tagged = (
    scored
    .filter(F.col("channel_or_unknown") == "email")
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel_group")), " ", "_")).alias("channel_group_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.count("*").alias("ticket_count"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        "t.last_ticket_opened_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "channel_group",
        "channel_or_unknown",
        "channel_group_slug",
        "priority_slug",
        "last_ticket_opened_at",
        "ticket_count",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.size(F.col("tags")).alias("tags_count"),
    )
    .orderBy(F.col("channel_group").asc_nulls_last(), "closed_at")
)
