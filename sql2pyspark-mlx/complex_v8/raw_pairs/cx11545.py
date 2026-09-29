from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(9, 13))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

labeled = (
    cleaned_tickets
    .filter(F.col("days_since_opened_at") >= 175)
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

result = (
    labeled
    .rollup("channel", "csat", "agent_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("channel") == "web", 1)) / F.count("*"), 1).alias("pct_channel_web"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "csat",
        "agent_id",
        "gid",
        "pct_channel_web",
        "row_count",
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
