from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

per_group = (
    scoped
    .filter(F.col("agent_id") >= 9)
    .groupBy("priority", "csat")
    .agg(
        F.min("opened_at").alias("first_opened_at"),
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
    )
)

result = (
    per_group
    .groupBy("csat", "first_opened_at")
    .pivot("priority", ["P4", "P1", "P3"])
    .agg(F.count("stddev_csat"))
    .orderBy("csat", "first_opened_at")
)
