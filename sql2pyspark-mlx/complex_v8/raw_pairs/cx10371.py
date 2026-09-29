from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_csat = Window.partitionBy("csat", "agent_id").orderBy("csat", "id")
w_priority = Window.partitionBy("priority")
w_opened_at_month_priority_by = Window.partitionBy(F.month("opened_at"), "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel = Window.partitionBy("channel")
w_channel_by_opened_at = Window.partitionBy("channel").orderBy(F.col("opened_at").desc(), "id")
w_csat_agent_id_by_csat_full = Window.partitionBy("csat", "agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_requester_id_pri = Window.orderBy(F.col("next_requester_id").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_csat_rank_by_rows_in_channel = Window.partitionBy("csat_rank").orderBy(F.col("rows_in_channel").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_rank_by_rows_in_channel_2 = Window.partitionBy("csat_rank").orderBy(F.col("rows_in_channel").desc(), "id")

prepared_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

with_trends = (
    prepared_tickets
    .select(
        "*",
        F.rank().over(w_csat_agent_id_by_csat).alias("csat_rank"),
        F.sum("requester_id").over(w_priority).alias("priority_sum_requester_id"),
        F.sum("csat_per_requester_id").over(w_opened_at_month_priority_by).alias("running_csat_per_requester_id"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round(F.cume_dist().over(w_channel_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.lead("requester_id").over(w_csat_agent_id_by_csat).alias("next_requester_id"),
        F.lead("csat_per_requester_id", 3).over(w_csat_agent_id_by_csat).alias("next3_csat_per_requester_id"),
        F.nth_value("csat", 2).over(w_csat_agent_id_by_csat_full).alias("second_csat"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.sum("priority_sum_requester_id").over(w_all_by_next_requester_id_pri).alias("priority_sum_requester_id_sum_2_prior"),
        F.round(F.col("rows_in_channel") - F.avg("rows_in_channel").over(w_channel), 2).alias("rows_in_channel_vs_channel_avg"),
        F.nth_value("next3_csat_per_requester_id", 2).over(w_csat_rank_by_rows_in_channel).alias("second_next3_csat_per_requester_id"),
        F.lag("next3_csat_per_requester_id", 3).over(w_csat_rank_by_rows_in_channel_2).alias("prev3_next3_csat_per_requester_id"),
        F.round(F.cume_dist().over(w_csat_rank_by_rows_in_channel_2), 3).alias("rows_in_channel_cume_dist"),
        F.lead("priority_sum_requester_id", 2).over(w_csat_rank_by_rows_in_channel_2).alias("next2_priority_sum_requester_id"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "csat_per_requester_id",
        "priority_sum_requester_id",
        "running_csat_per_requester_id",
        "rows_in_channel",
        "opened_at_cume_dist",
        "next_requester_id",
        "next3_csat_per_requester_id",
        "second_csat",
        "second_next3_csat_per_requester_id",
        "prev3_next3_csat_per_requester_id",
        "rows_in_channel_cume_dist",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
)
