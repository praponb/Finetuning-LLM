from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_or_unknown_agent_id = Window.partitionBy("priority_or_unknown", "agent_id").orderBy("requester_id", "id")
w_channel_len = Window.partitionBy("channel_len")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_priority_or_unknown_agent_id_2 = Window.partitionBy("priority_or_unknown", "agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter((F.col("priority").isNotNull()) | (F.col("closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.when(F.col("channel").isin("email", "chat"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        (F.col("channel_len") - F.lag("channel_len").over(w_priority_or_unknown_agent_id)).alias("channel_len_change"),
        F.count(F.lit(1)).over(w_channel_len).alias("rows_in_channel_len"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.round(F.avg("channel_len").over(w_channel_len), 2).alias("channel_len_avg_channel_len"),
        F.last("requester_id").over(w_priority_or_unknown_agent_id_2).alias("last_requester_id"),
    )
)

result = (
    windowed
    .cube("channel_len", "priority_or_unknown", "agent_id")
    .agg(F.count("*").alias("row_count"))
    .select(
        "channel_len",
        F.coalesce(F.col("priority_or_unknown"), F.lit("ALL")).alias("priority_or_unknown"),
        "agent_id",
        "row_count",
    )
    .orderBy(
        F.col("channel_len").asc_nulls_last(),
        F.col("priority_or_unknown").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
