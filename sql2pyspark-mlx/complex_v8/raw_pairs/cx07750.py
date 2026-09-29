from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("priority") == "P2") & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

tagged = (
    recent
    .select(
        "*",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.when(F.col("requester_id") >= 22, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

enriched = (
    tagged
    .filter(F.col("requester_id_tier").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel_slug")), " ", "_")).alias("channel_slug_slug"),
        F.concat_ws("|", F.col("priority"), F.col("requester_id_tier")).alias("priority_requester_id_tier_label"),
        F.length(F.trim(F.col("channel_slug"))).alias("channel_slug_len"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "chat"), "left_semi")
)

result = (
    with_activity
    .groupingSets(
        [["channel", "requester_id_tier", "priority"], ["channel"], ["requester_id_tier"]],
        "channel", "requester_id_tier", "priority",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum("channel_slug_len").alias("total_channel_slug_len"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "cnt",
        "total_channel_slug_len",
        "distinct_ids",
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("requester_id_tier").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
