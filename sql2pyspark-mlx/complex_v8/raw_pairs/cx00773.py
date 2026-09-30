from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc_nulls_first())
w_agent_tier_agent_id = Window.partitionBy("agent_tier", "agent_id")
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id")
w_channel_by_closed_at_moving = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)
w_channel_by_closed_at_full = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier = Window.partitionBy("agent_tier")
w_all_by_closed_at_prior_6 = Window.orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_closed_at_to_date_by_opened = Window.partitionBy(F.to_date("closed_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_closed_at_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.month(F.col("closed_at")).isin(2, 3)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

with_parent_agent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_parent_agent
    .select(
        "*",
        F.rank().over(w_all_by_opened_at_ties).alias("opened_at_rank"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.size(F.collect_set("priority").over(w_agent_tier_agent_id)).alias("distinct_priority_in_agent_tier_agent_id"),
        F.dense_rank().over(w_agent_id_by_requester_id).alias("requester_id_dense_rank"),
        F.round(F.avg("requester_id").over(w_channel_by_closed_at_moving), 2).alias("requester_id_avg_3"),
        F.first("closed_at").over(w_channel_by_closed_at_full).alias("first_closed_at"),
        F.round(F.avg("requester_id").over(w_agent_tier), 2).alias("agent_tier_avg_requester_id"),
        F.round(F.avg("requester_id").over(w_all_by_closed_at_prior_6), 2).alias("requester_id_avg_6_prior"),
        F.sum("requester_id").over(w_agent_tier).alias("agent_tier_sum_requester_id"),
        F.sum("requester_id").over(w_closed_at_to_date_by_opened).alias("running_requester_id"),
        F.sum("requester_id").over(w_channel_by_closed_at_running).alias("running_requester_id_2"),
        F.min("requester_id").over(w_agent_tier).alias("agent_tier_min_requester_id"),
    )
)

result = (
    metrics
    .filter(F.col("agent_id") >= 6)
    .select(
        "id",
        "agent_id",
        "agent_tier",
        "opened_at_rank",
        "opened_at_dense_rank",
        "distinct_priority_in_agent_tier_agent_id",
        "requester_id_dense_rank",
        "requester_id_avg_3",
        "first_closed_at",
        "agent_tier_avg_requester_id",
        "requester_id_avg_6_prior",
        "agent_tier_sum_requester_id",
        "running_requester_id",
        "running_requester_id_2",
        F.coalesce(F.col("agent_tier"), F.lit("UNKNOWN")).alias("agent_tier_or_unknown"),
    )
    .orderBy(
        F.col("first_closed_at").desc(),
        F.col("agent_tier_sum_requester_id").desc(),
    )
)
