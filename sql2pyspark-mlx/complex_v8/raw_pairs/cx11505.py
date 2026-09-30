from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "agent_id").orderBy("opened_at", "id")

valid_tickets = (
    df_tickets
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

latest_per_key = (
    valid_tickets
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest_per_key
    .groupBy("priority", "channel")
    .agg(
        F.count(F.when(F.col("channel").isin("web", "phone", "chat"), 1)).alias("n_channel_in_web_phone_chat"),
        F.min("closed_at").alias("first_closed_at"),
        F.sum("csat").alias("total_csat"),
        F.max("csat").alias("max_csat"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.min("requester_id").alias("min_requester_id"),
    )
)

result = (
    stats
    .groupingSets(
        [["channel", "max_csat", "n_channel_in_web_phone_chat"], ["channel"], ["max_csat"], []],
        "channel", "max_csat", "n_channel_in_web_phone_chat",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("max_csat", 0.5).alias("median_max_csat"),
        F.max("min_requester_id").alias("max_min_requester_id"),
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("max_csat").asc_nulls_last(),
        F.col("n_channel_in_web_phone_chat").asc_nulls_last(),
    )
)
