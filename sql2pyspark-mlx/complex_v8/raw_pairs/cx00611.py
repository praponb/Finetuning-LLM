from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_channel_priority_by_csat_ful = Window.partitionBy("channel", "priority").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id")
w_channel_priority_by_csat_pri = Window.partitionBy("channel", "priority").orderBy("csat", "id").rowsBetween(-2, -1)
w_priority_by_csat_moving_2 = Window.partitionBy("priority").orderBy(F.col("csat").desc(), "id").rowsBetween(-2, Window.currentRow)
w_agent_id_channel_by_csat_tie = Window.partitionBy("agent_id", "channel").orderBy(F.col("csat").asc_nulls_last())
w_channel = Window.partitionBy("channel")
w_agent_id_channel_by_csat_run = Window.partitionBy("agent_id", "channel").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_rank = Window.orderBy("opened_at_rank", "id")
w_csat_change_csat_dense_rank = Window.partitionBy("csat_change", "csat_dense_rank")
w_all_by_opened_at_rank_runnin = Window.orderBy("opened_at_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_change_prior_6 = Window.orderBy(F.col("csat_change").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_csat_change = Window.orderBy(F.col("csat_change").desc_nulls_first(), "id")

cleaned_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) | (F.col("agent_id") <= 6))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
    )
)

with_trends = (
    cleaned_tickets
    .select(
        "id",
        "opened_at",
        "channel",
        "csat",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.row_number().over(w_all_by_opened_at).alias("rn"),
        F.first("requester_id").over(w_channel_priority_by_csat_ful).alias("first_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_priority_by_csat)).alias("csat_change"),
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.max("requester_id").over(w_csat).alias("csat_max_requester_id"),
        F.round(F.avg("csat").over(w_channel_priority_by_csat_pri), 2).alias("csat_avg_2_prior"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_opened_at)).alias("requester_id_change"),
        F.max("csat").over(w_priority_by_csat_moving_2).alias("csat_max_3"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat), 2).alias("requester_id_vs_csat_avg"),
        F.dense_rank().over(w_agent_id_channel_by_csat_tie).alias("csat_dense_rank"),
        F.lead("requester_id", 2).over(w_all_by_opened_at).alias("next2_requester_id"),
        F.min("requester_id").over(w_channel).alias("channel_min_requester_id"),
        F.round(F.avg("csat").over(w_csat), 2).alias("csat_avg_csat"),
        F.sum("csat").over(w_agent_id_channel_by_csat_run).alias("running_csat"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.lead("running_csat").over(w_all_by_opened_at_rank).alias("next_running_csat"),
        F.round(F.col("csat_share_of_csat") - F.avg("csat_share_of_csat").over(w_csat_change_csat_dense_rank), 2).alias("csat_share_of_csat_vs_csat_change_csat_dense_ran"),
        F.sum("opened_at_rank").over(w_all_by_opened_at_rank_runnin).alias("running_opened_at_rank"),
        F.round(F.avg("requester_id_vs_csat_avg").over(w_all_by_csat_change_prior_6), 2).alias("requester_id_vs_csat_avg_avg_6_prior"),
        F.lead("first_requester_id").over(w_all_by_csat_change).alias("next_first_requester_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "channel",
        "first_requester_id",
        "csat_change",
        "opened_at_rank",
        "csat_max_requester_id",
        "csat_avg_2_prior",
        "requester_id_change",
        "csat_max_3",
        "requester_id_vs_csat_avg",
        "next2_requester_id",
        "channel_min_requester_id",
        "running_csat",
        "csat_share_of_csat_vs_csat_change_csat_dense_ran",
        "requester_id_vs_csat_avg_avg_6_prior",
        "next_first_requester_id",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
    .orderBy(F.col("first_requester_id").desc())
)
