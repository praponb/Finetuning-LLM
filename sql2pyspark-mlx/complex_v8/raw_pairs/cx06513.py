from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.abs(F.col("requester_id") - 8).alias("requester_id_distance"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

qualified = (
    filtered_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "upgrade")), "left_anti")
    .filter(F.col("agent_id").isNotNull())
)

grouped = (
    qualified
    .filter(~F.col("channel").isin("web", "phone"))
    .groupBy(
        "priority_or_unknown",
        F.date_format("closed_at", "yyyy-MM").alias("closed_at_ym"),
    )
    .agg(
        F.round(F.stddev("requester_id"), 2).alias("stddev_requester_id"),
        F.sum(F.when(F.col("channel") == "web", F.col("days_since_opened_at")).otherwise(0)).alias("days_since_opened_at_channel_web"),
        F.round(100 * F.count(F.when(F.col("channel") == "email", 1)) / F.count("*"), 1).alias("pct_channel_email"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)

annotated = (
    grouped
    .filter(F.col("days_since_opened_at_channel_web") <= 2450)
    .withColumn("pct_channel_email_adj", F.round(F.col("pct_channel_email") * 0.9, 2))
)

tagged = (
    annotated
    .filter(F.col("days_since_opened_at_channel_web") >= 1400)
    .select(
        "*",
        F.round(F.col("total_requester_id") / F.nullif(F.col("stddev_requester_id"), F.lit(0)), 3).alias("total_requester_id_per_stddev_requester_id"),
        F.least(F.col("pct_channel_email_adj"), F.col("total_requester_id")).alias("least_pct_channel_email_adj_total_requester_id"),
    )
)

result = (
    tagged
    .filter(F.col("closed_at_ym").isNotNull())
    .select(
        "priority_or_unknown",
        "closed_at_ym",
        "stddev_requester_id",
        "days_since_opened_at_channel_web",
        "pct_channel_email",
        "total_requester_id_per_stddev_requester_id",
        "least_pct_channel_email_adj_total_requester_id",
        (F.floor(F.col("total_requester_id") / 25) * 25).alias("total_requester_id_bucket"),
    )
    .orderBy("priority_or_unknown")
)
