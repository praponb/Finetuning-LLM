from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id")
w_closed_at_month_agent_id_by = Window.partitionBy(F.month("closed_at"), "agent_id").orderBy("csat", "id").rowsBetween(-3, -1)

recent = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.abs(F.col("requester_id") - 8).alias("requester_id_distance"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.dense_rank().over(w_channel_agent_id_by_closed_a).alias("closed_at_dense_rank"),
        F.sum("csat").over(w_closed_at_month_agent_id_by).alias("csat_sum_3_prior"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.sum("requester_id_distance").over(w_agent_id).alias("agent_id_sum_requester_id_distance"),
        F.round(F.col("requester_id_distance") / F.nullif(F.sum("requester_id_distance").over(w_agent_id), F.lit(0)), 4).alias("requester_id_distance_share_of_agent_id"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.priority_prefix",
        "s.requester_id_distance",
        "s.distinct_channel_in_agent_id",
        "s.closed_at_dense_rank",
        "s.csat_sum_3_prior",
        "s.agent_id_sum_requester_id_distance",
        "s.requester_id_distance_share_of_agent_id",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details
    .rollup("agent_team", "distinct_channel_in_agent_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
