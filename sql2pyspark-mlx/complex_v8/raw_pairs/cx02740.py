from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "tags",
    )
)

stats = (
    cleaned_tickets
    .groupBy("agent_id", "csat", "priority")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("requester_id"), 2).alias("avg_requester_id"),
    )
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.avg("avg_requester_id").alias("overall_avg_avg_requester_id")))
    .filter(F.col("avg_requester_id") > F.col("overall_avg_avg_requester_id"))
    .drop("overall_avg_avg_requester_id")
)

result = (
    with_activity
    .select(
        "agent_id",
        "priority",
        "avg_requester_id",
        F.when(F.col("cnt") >= 9, "HIGH").when(F.col("cnt") >= 4, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)
