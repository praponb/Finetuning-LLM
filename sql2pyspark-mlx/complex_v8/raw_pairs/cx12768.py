from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_agent_id_by_opened_at_ties = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc())
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_priority_group_agent_id_by_m = Window.partitionBy("priority_group", "agent_id").orderBy("minutes_opened_at_to_closed_at", "id")
w_agent_id_by_closed_at_prior = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, -1)
w_agent_id_by_opened_at_full = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_group_agent_id_by_m_2 = Window.partitionBy("priority_group", "agent_id").orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_requester_id = Window.partitionBy(F.month("opened_at"), "requester_id_tercile")
w_distinct_channel_in_priority = Window.partitionBy("distinct_channel_in_priority", "priority_group").orderBy("minutes_opened_at_to_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_channel_in_priority_2 = Window.partitionBy("distinct_channel_in_priority", "agent_id").orderBy("requester_id_percent_rank", "id")
w_distinct_channel_in_priority_3 = Window.partitionBy("distinct_channel_in_priority", "priority_group").orderBy("minutes_opened_at_to_closed_at")
w_all_by_distinct_channel_in_p = Window.orderBy("distinct_channel_in_priority", "id")
w_all_by_n_priority_p4_prior_3 = Window.orderBy("n_priority_p4", "requester_id_seq").rowsBetween(-3, -1)
w_all_by_n_priority_p4 = Window.orderBy("n_priority_p4", "requester_id_seq")

cleaned = (
    df_tickets
    .filter(F.col("agent_id") <= 8)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "priority_group",
        "minutes_opened_at_to_closed_at",
        F.size(F.collect_set("channel").over(w_priority)).alias("distinct_channel_in_priority"),
        F.rank().over(w_agent_id_by_opened_at_ties).alias("opened_at_rank"),
        F.rank().over(w_all_by_requester_id).alias("requester_id_rank"),
        F.dense_rank().over(w_all_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.ntile(3).over(w_all_by_requester_id).alias("requester_id_tercile"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority), F.lit(0)), 4).alias("requester_id_share_of_priority"),
        F.row_number().over(w_all_by_requester_id).alias("requester_id_seq"),
        F.lag("opened_at").over(w_priority_group_agent_id_by_m).alias("prev_opened_at"),
        F.max("requester_id").over(w_agent_id_by_closed_at_prior).alias("requester_id_max_6_prior"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.nth_value("requester_id", 2).over(w_agent_id_by_opened_at_full).alias("second_requester_id"),
        F.first("requester_id", ignorenulls=True).over(w_priority_group_agent_id_by_m_2).alias("first_known_requester_id"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("requester_id_dense_rank") <= 3)
    .drop("requester_id_dense_rank")
)

metrics = (
    top_per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_opened_at_month_requester_id).alias("rows_in_opened_at_month_requester_id_tercile"),
        F.last("requester_id_tercile").over(w_distinct_channel_in_priority).alias("last_requester_id_tercile"),
        F.row_number().over(w_distinct_channel_in_priority_2).alias("rn"),
        F.round(F.cume_dist().over(w_distinct_channel_in_priority_3), 3).alias("minutes_opened_at_to_closed_at_cume_dist"),
        F.lead("distinct_channel_in_priority").over(w_all_by_distinct_channel_in_p).alias("next_distinct_channel_in_priority"),
    )
)

grouped = (
    metrics
    .filter(F.hour(F.col("prev_opened_at")).between(6, 10))
    .groupBy("requester_id_seq")
    .agg(
        F.count(F.when(F.col("priority") != "P4", 1)).alias("n_priority_p4"),
        F.sum(F.when(F.col("priority") == "P1", F.col("rows_in_opened_at_month_requester_id_tercile")).otherwise(0)).alias("rows_in_opened_at_month_requester_id_tercile_pri"),
        F.max("requester_id_rank").alias("max_requester_id_rank_check"),
    )
    .filter(F.col("max_requester_id_rank_check") > 4)
    .drop("max_requester_id_rank_check")
)

with_trends = (
    grouped
    .select(
        "requester_id_seq",
        "n_priority_p4",
        F.round(F.avg("n_priority_p4").over(w_all_by_n_priority_p4_prior_3), 2).alias("n_priority_p4_avg_3_prior"),
        F.lead("requester_id_seq").over(w_all_by_n_priority_p4).alias("next_requester_id_seq"),
    )
)

result = (
    with_trends
    .filter(F.col("n_priority_p4_avg_3_prior") < 8.5)
    .select("next_requester_id_seq")
    .distinct()
    .orderBy(F.col("next_requester_id_seq").desc_nulls_first())
)
