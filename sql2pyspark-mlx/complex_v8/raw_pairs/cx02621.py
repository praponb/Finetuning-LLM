from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("agent_id").isNotNull()))
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
        F.abs(F.col("csat") - 3).alias("csat_distance"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
        F.when(F.col("csat") >= 3, "HIGH").when(F.col("csat") >= 2, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T2", "T1")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.csat_distance",
        "s.channel_len",
        "s.csat_tier",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_parent
    .rollup("channel_len", "csat_distance")
    .agg(
        F.grouping("channel_len").alias("is_total_channel_len"),
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
        F.round(F.avg("channel_len"), 2).alias("avg_channel_len"),
        F.sum("channel_len").alias("total_channel_len"),
    )
    .orderBy(
        F.col("channel_len").asc_nulls_last(),
        F.col("csat_distance").asc_nulls_last(),
    )
)
