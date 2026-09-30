from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
)

tagged = (
    prepared_tickets
    .filter(F.col("channel") == "web")
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.concat_ws("|", F.col("priority_ref"), F.col("channel")).alias("priority_ref_channel_label"),
        F.coalesce(F.col("days_since_closed_at"), F.lit(0)).alias("days_since_closed_at_filled"),
    )
)

result = (
    tagged
    .cube("agent_id", "channel")
    .agg(
        F.round(F.avg("days_since_closed_at"), 2).alias("avg_days_since_closed_at"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "avg_days_since_closed_at",
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), F.col("channel").asc_nulls_last())
)
