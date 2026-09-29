from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_opened_a = Window.partitionBy("channel", "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_agent_id_by_opened_a_2 = Window.partitionBy("channel", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy(F.col("requester_id").desc())

cleaned_tickets = (
    df_tickets
    .filter(F.col("csat") >= 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
    )
)

with_windows = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.dense_rank().over(w_channel_agent_id_by_opened_a).alias("opened_at_dense_rank"),
        F.size(F.collect_set("priority").over(w_agent_id_channel)).alias("distinct_priority_in_agent_id_channel"),
        F.last("requester_id").over(w_channel_agent_id_by_opened_a_2).alias("last_requester_id"),
        F.row_number().over(w_channel_agent_id_by_opened_a).alias("rn"),
        F.round(F.cume_dist().over(w_channel_agent_id_by_requeste), 3).alias("requester_id_cume_dist"),
        F.min("requester_id").over(w_agent_id_channel).alias("agent_id_channel_min_requester_id"),
    )
)

result = (
    with_windows
    .cube("distinct_priority_in_agent_id_channel", "channel", "opened_at_dense_rank")
    .agg(
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.round(F.sum("opened_at_dense_rank") / F.nullif(F.sum("agent_id_channel_min_requester_id"), F.lit(0)), 3).alias("opened_at_dense_rank_to_agent_id_channel_min_req"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("distinct_priority_in_agent_id_channel"), F.lit(0)), 3).alias("requester_id_to_distinct_priority_in_agent_id_ch"),
    )
    .select(
        "distinct_priority_in_agent_id_channel",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "opened_at_dense_rank",
        "distinct_prioritys",
        "opened_at_dense_rank_to_agent_id_channel_min_req",
        "requester_id_to_distinct_priority_in_agent_id_ch",
    )
    .orderBy(
        F.col("distinct_priority_in_agent_id_channel").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
        F.col("opened_at_dense_rank").asc_nulls_last(),
    )
)
