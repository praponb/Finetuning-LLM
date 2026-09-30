from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_requester_id_tier_b = Window.partitionBy("priority", "requester_id_tier").orderBy(F.col("closed_at").desc_nulls_first())
w_priority_requester_id_tier_b_2 = Window.partitionBy("priority", "requester_id_tier").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_priority_by_row_count = Window.partitionBy("priority").orderBy(F.col("row_count").asc_nulls_last(), "priority")
w_priority_by_row_count_ties = Window.partitionBy("priority").orderBy(F.col("row_count").asc_nulls_last())
w_row_count_n_channel_is_not_n = Window.partitionBy("row_count", "n_channel_is_not_null")

cleaned = (
    df_tickets
    .filter(F.col("requester_id") <= 17)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 17, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("channel") == "chat")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.dense_rank().over(w_priority_requester_id_tier_b).alias("closed_at_dense_rank"),
        F.round(F.percent_rank().over(w_priority_requester_id_tier_b_2), 3).alias("closed_at_percent_rank"),
        F.dense_rank().over(w_priority_requester_id_tier_b_2).alias("closed_at_dense_rank_2"),
        F.ntile(4).over(w_priority_requester_id_tier_b_2).alias("closed_at_quartile"),
        F.sum("csat").over(w_priority_agent_id).alias("priority_agent_id_sum_csat"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel").isin("chat", "web", "email")), "left_semi")
)

grouped = (
    without_matches
    .groupBy("priority")
    .agg(
        F.sum("closed_at_dense_rank").alias("total_closed_at_dense_rank"),
        F.round(F.avg("closed_at_percent_rank"), 2).alias("avg_closed_at_percent_rank"),
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("n_channel_is_not_null"),
        F.count("*").alias("row_count"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.row_number().over(w_priority_by_row_count).alias("row_count_seq"),
        F.round(F.cume_dist().over(w_priority_by_row_count_ties), 3).alias("row_count_cume_dist"),
        F.round(F.col("avg_closed_at_percent_rank") / F.nullif(F.sum("avg_closed_at_percent_rank").over(w_row_count_n_channel_is_not_n), F.lit(0)), 4).alias("avg_closed_at_percent_rank_share_of_row_count_n_"),
    )
)

result = (
    ranked
    .select(
        "priority",
        "total_closed_at_dense_rank",
        "n_channel_is_not_null",
        "row_count",
        "avg_closed_at_percent_rank_share_of_row_count_n_",
    )
    .orderBy(F.col("n_channel_is_not_null").desc())
)
