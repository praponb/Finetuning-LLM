from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("channel").isNotNull()) & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

labeled = (
    valid_tickets
    .filter(F.col("channel") == "phone")
    .withColumn("requester_id_bucket_distance", F.abs(F.col("requester_id_bucket") - 18))
)

result = (
    labeled
    .groupBy("opened_at")
    .pivot("channel", ["web", "phone"])
    .agg(F.count("requester_id_bucket_distance"))
    .orderBy("opened_at")
)
