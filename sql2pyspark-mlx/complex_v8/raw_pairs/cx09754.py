from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_requester_id = Window.partitionBy("channel").orderBy(F.col("requester_id").desc(), "id")
w_channel_by_requester_id_runn = Window.partitionBy("channel").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")

recent = (
    df_tickets
    .filter((F.col("priority").isNotNull()) & (F.col("requester_id") <= 14))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

enriched_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.channel_priority_label",
        "s.priority_channel_label",
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    enriched_agent
    .select(
        "*",
        F.dense_rank().over(w_channel_by_requester_id).alias("requester_id_dense_rank"),
        F.count(F.lit(1)).over(w_channel_by_requester_id_runn).alias("running_count"),
        F.sum("requester_id").over(w_agent_id).alias("agent_id_sum_requester_id"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "channel_priority_label",
        "priority_channel_label",
        "agent_team",
        "requester_id_dense_rank",
        "running_count",
        "agent_id_sum_requester_id",
        "distinct_priority_in_agent_id",
        (F.floor(F.col("requester_id_dense_rank") / 2) * 2).alias("requester_id_dense_rank_bucket"),
    )
    .orderBy(F.col("opened_at").desc())
)
