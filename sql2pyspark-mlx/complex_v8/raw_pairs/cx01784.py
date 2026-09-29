from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_all_by_opened_at_prior_1 = Window.orderBy("opened_at", "id").rowsBetween(-1, -1)
w_csat_by_opened_at_full = Window.partitionBy("csat").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_csat_by_closed_at_fu = Window.partitionBy("channel", "csat").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_priority_channel_by_csat = Window.partitionBy("priority", "channel").orderBy("csat", "id")
w_all_by_csat = Window.orderBy("csat", "id")
w_csat_priority_by_closed_at_f = Window.partitionBy("csat", "priority").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_priority_by_closed_at = Window.partitionBy("csat", "priority").orderBy(F.col("closed_at").desc(), "id")
w_all_by_csat_ties = Window.orderBy("csat")
w_channel_csat_by_closed_at = Window.partitionBy("channel", "csat").orderBy("closed_at", "id")

prepared = df_tickets

joined_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

with_windows = (
    joined_agent
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.round(F.avg("requester_id").over(w_all_by_opened_at_prior_1), 2).alias("requester_id_avg_1_prior"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.last("opened_at").over(w_csat_by_opened_at_full).alias("last_opened_at"),
        F.last("csat").over(w_channel_csat_by_closed_at_fu).alias("last_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
        (F.col("csat") - F.lag("csat").over(w_priority_channel_by_csat)).alias("csat_change"),
        F.row_number().over(w_all_by_csat).alias("csat_seq"),
        F.first("csat").over(w_csat_priority_by_closed_at_f).alias("first_csat"),
        F.round(F.avg("csat").over(w_channel), 2).alias("channel_avg_csat"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.dense_rank().over(w_csat_priority_by_closed_at).alias("closed_at_dense_rank"),
        F.size(F.collect_set("priority").over(w_agent_id)).alias("distinct_priority_in_agent_id"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.row_number().over(w_channel_csat_by_closed_at).alias("closed_at_seq"),
    )
)

first_rows = (
    with_windows
    .filter(F.col("closed_at_seq") <= 5)
    .drop("closed_at_seq")
)

result = (
    first_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        "agent_team",
        "distinct_agent_id_in_channel",
        "requester_id_avg_1_prior",
        "last_opened_at",
        "requester_id_share_of_agent_id",
        "agent_id_min_csat",
        "first_csat",
        "channel_avg_csat",
        "csat_rank",
        "closed_at_dense_rank",
        "csat_percent_rank",
        F.when(F.col("channel").isin("web", "phone"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
)
