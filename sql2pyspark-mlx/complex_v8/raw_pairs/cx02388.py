from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "refund"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.size(F.col("tags")).alias("tags_count"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

channel_stats = (
    filtered
    .groupBy("channel")
    .agg(F.percentile_approx("requester_id", 0.5).alias("median_requester_id"))
)

compared = (
    filtered.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.requester_id") >= F.col("g.median_requester_id"))
    .select(
        "s.*",
        "g.median_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.median_requester_id"), 2).alias("requester_id_minus_median"),
    )
)

result = (
    compared
    .rollup("channel", "agent_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("channel") == "chat", 1).otherwise(0)).alias("n_channel_chat"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "agent_id",
        "gid",
        "n_channel_chat",
        "distinct_channels",
    )
    .orderBy(F.col("channel").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
