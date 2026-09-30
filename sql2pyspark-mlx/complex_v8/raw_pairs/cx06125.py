from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(~F.col("channel").isin("email", "web"))
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
    )
)

stats = (
    scoped
    .groupBy("channel", "agent_id")
    .agg(
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum(F.when(F.col("channel").isNotNull(), F.col("requester_id")).otherwise(0)).alias("requester_id_channel_is_not_null"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)

result = (
    stats
    .groupBy("distinct_channels", "agent_id")
    .pivot("channel", ["phone", "email", "chat"])
    .agg(F.count("requester_id_channel_is_not_null").alias("count_v"), F.sum("requester_id_channel_is_not_null").alias("sum_v"))
)
