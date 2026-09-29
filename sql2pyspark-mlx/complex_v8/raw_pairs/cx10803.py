from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-24 01:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

stats = (
    recent
    .groupBy("priority", F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.min("days_since_closed_at").alias("min_days_since_closed_at"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("days_since_closed_at"), F.lit(0)), 3).alias("requester_id_to_days_since_closed_at_ratio"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.count("*").alias("cnt"),
    )
)

scored = (
    stats
    .filter(F.col("opened_at_ym").isNotNull())
    .select("*", F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"))
)

qualified = (
    scored
    .crossJoin(scored.agg(F.avg("min_days_since_closed_at").alias("overall_avg_min_days_since_closed_at")))
    .filter(F.col("min_days_since_closed_at") > F.col("overall_avg_min_days_since_closed_at"))
    .drop("overall_avg_min_days_since_closed_at")
)

result = (
    qualified
    .groupBy("cnt", "cnt_filled")
    .pivot("priority", ["P3", "P4", "P2", "P1"])
    .agg(F.avg("requester_id_to_days_since_closed_at_ratio"))
    .orderBy("cnt", "cnt_filled")
)
