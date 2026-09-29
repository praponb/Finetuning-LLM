from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("requester_id").between(7, 25)) | (F.to_date(F.col("closed_at")) < F.lit("2025-02-10").cast("date")))
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
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

scored = (
    valid
    .filter(F.col("csat") > 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "channel_prefix",
        "csat_per_requester_id",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        (F.floor(F.col("csat_per_requester_id") / 2) * 2).alias("csat_per_requester_id_bucket"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.channel_prefix",
        "s.channel_clean",
        "s.csat_per_requester_id_bucket",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    enriched
    .groupingSets(
        [["agent_tier", "agent_id", "csat_per_requester_id_bucket"], ["agent_tier"], ["agent_id"], []],
        "agent_tier", "agent_id", "csat_per_requester_id_bucket",
    )
    .agg(
        F.sum(F.when(F.col("agent_tier").isin("T1", "T2"), F.col("csat_per_requester_id_bucket")).otherwise(0)).alias("csat_per_requester_id_bucket_agent_tier_in_t1_t2"),
        F.max("closed_at").alias("last_closed_at"),
        F.round(F.stddev("requester_id"), 2).alias("stddev_requester_id"),
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("csat_per_requester_id_bucket").asc_nulls_last(),
    )
)
