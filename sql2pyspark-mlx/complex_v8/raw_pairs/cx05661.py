from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

grouped = (
    recent
    .groupBy(
        F.date_trunc("month", "closed_at").alias("closed_at_month"),
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.sum(F.when(F.col("priority").isin("P1", "P2"), 1).otherwise(0)).alias("n_priority_in_p1_p2"),
        F.sum("csat").alias("total_csat"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.sum("days_since_closed_at").alias("total_days_since_closed_at"),
        F.max("days_since_closed_at").alias("max_days_since_closed_at"),
        F.round(F.sum("days_since_closed_at") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("days_since_closed_at_to_requester_id_ratio"),
        F.avg("requester_id").alias("avg_requester_id_check"),
    )
    .filter(F.col("avg_requester_id_check") > 7)
    .drop("avg_requester_id_check")
)

qualified = (
    grouped
    .crossJoin(grouped.agg(F.min("avg_csat").alias("overall_min_avg_csat")))
    .filter(F.col("avg_csat") > F.col("overall_min_avg_csat"))
    .drop("overall_min_avg_csat")
)

enriched = (
    qualified
    .select(
        "*",
        F.when(F.col("total_csat") >= 21, "large").when(F.col("total_csat") >= 11, "medium").otherwise("small").alias("total_csat_tier"),
        F.coalesce(F.col("total_csat"), F.lit(0)).alias("total_csat_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at_month")).alias("days_since_closed_at_month"),
    )
)

tagged = (
    enriched
    .select(
        "closed_at_month",
        "opened_at_month",
        "total_csat",
        "avg_csat",
        "total_days_since_closed_at",
        "max_days_since_closed_at",
        "days_since_closed_at_to_requester_id_ratio",
        "total_csat_tier",
        "total_csat_filled",
        "days_since_closed_at_month",
        F.abs(F.col("max_days_since_closed_at") - 210).alias("max_days_since_closed_at_distance"),
        F.when(F.col("total_csat") >= 17, "HIGH").when(F.col("total_csat") >= 14, "MEDIUM").otherwise("LOW").alias("total_csat_tier_2"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.min("total_days_since_closed_at").alias("overall_min_total_days_since_closed_at")))
    .filter(F.col("total_days_since_closed_at") > F.col("overall_min_total_days_since_closed_at"))
    .drop("overall_min_total_days_since_closed_at")
)

scored = (
    with_activity
    .select(
        "closed_at_month",
        "opened_at_month",
        "avg_csat",
        "total_days_since_closed_at",
        "max_days_since_closed_at",
        "days_since_closed_at_to_requester_id_ratio",
        "total_csat_tier",
        "total_csat_filled",
        "days_since_closed_at_month",
        "max_days_since_closed_at_distance",
        "total_csat_tier_2",
        F.coalesce(F.col("avg_csat"), F.lit(0.0)).alias("avg_csat_filled"),
        F.coalesce(F.col("total_csat_tier"), F.lit("UNKNOWN")).alias("total_csat_tier_or_unknown"),
    )
)

result = (
    scored
    .filter(F.col("total_csat_tier_or_unknown").isin("medium", "UNKNOWN"))
    .select(
        "closed_at_month",
        "avg_csat",
        "total_days_since_closed_at",
        "max_days_since_closed_at",
        "days_since_closed_at_to_requester_id_ratio",
        "total_csat_tier",
        "total_csat_filled",
        "days_since_closed_at_month",
        "max_days_since_closed_at_distance",
        "total_csat_tier_2",
        "avg_csat_filled",
    )
    .orderBy(F.col("total_csat_tier").desc())
)
