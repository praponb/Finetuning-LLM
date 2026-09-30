from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_prior_6 = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(-6, -1)
w_all_by_csat_running = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_by_requester_id_ties = Window.partitionBy("csat").orderBy(F.col("requester_id").asc_nulls_last())
w_all_by_requester_id_running = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_id_by_csat_run = Window.partitionBy("channel", "agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_prior_3 = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)
w_channel_agent_id_by_csat_tie = Window.partitionBy("channel", "agent_id").orderBy("csat")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_channel = Window.partitionBy("channel")
w_channel_agent_id_by_csat_ful = Window.partitionBy("channel", "agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_requester = Window.orderBy("first_known_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_known_requester_2 = Window.orderBy("first_known_requester_id", "id")
w_priority_or_unknown_prev3_cs = Window.partitionBy("priority_or_unknown", "prev3_csat")
w_requester_id_dense_rank_by_s = Window.partitionBy("requester_id_dense_rank").orderBy("second_requester_id", "id")
w_channel_csat_by_second_reque = Window.partitionBy("channel", "csat").orderBy("second_requester_id")

prepared_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (~F.col("priority").isin("P1", "P2")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

sequenced = (
    prepared_tickets
    .select(
        "*",
        F.sum("requester_id").over(w_all_by_csat_prior_6).alias("requester_id_sum_6_prior"),
        F.first("requester_id", ignorenulls=True).over(w_all_by_csat_running).alias("first_known_requester_id"),
        F.dense_rank().over(w_csat_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.first("closed_at", ignorenulls=True).over(w_all_by_requester_id_running).alias("first_known_closed_at"),
        F.last("csat", ignorenulls=True).over(w_channel_agent_id_by_csat_run).alias("last_known_csat"),
        F.round(F.avg("csat").over(w_all_by_csat_prior_3), 2).alias("csat_avg_3_prior"),
        F.count(F.lit(1)).over(w_all_by_csat_running).alias("running_count"),
        F.dense_rank().over(w_channel_agent_id_by_csat_tie).alias("csat_dense_rank"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat_agent_id), 2).alias("requester_id_vs_csat_agent_id_avg"),
        F.lag("csat", 3).over(w_all_by_requester_id).alias("prev3_csat"),
        F.min("csat").over(w_channel).alias("channel_min_csat"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_agent_id), F.lit(0)), 4).alias("csat_share_of_csat_agent_id"),
        F.size(F.collect_set("channel").over(w_csat_agent_id)).alias("distinct_channel_in_csat_agent_id"),
        F.row_number().over(w_all_by_requester_id).alias("rn"),
        F.nth_value("requester_id", 2).over(w_channel_agent_id_by_csat_ful).alias("second_requester_id"),
    )
)

scored = (
    sequenced
    .filter(F.col("csat_share_of_csat_agent_id") >= 0.3)
    .select(
        "*",
        F.round(F.col("csat_avg_3_prior") * 1.07, 2).alias("csat_avg_3_prior_adj"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix_2"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "closed_at",
        "csat",
        "channel_prefix",
        "first_known_requester_id",
        "first_known_closed_at",
        "last_known_csat",
        "running_count",
        "csat_dense_rank",
        "requester_id_vs_csat_agent_id_avg",
        "channel_min_csat",
        "rows_in_channel",
        "distinct_channel_in_csat_agent_id",
        "rn",
        "csat_avg_3_prior_adj",
        "priority_or_unknown",
        F.first("rows_in_channel", ignorenulls=True).over(w_all_by_first_known_requester).alias("first_known_rows_in_channel"),
        F.rank().over(w_all_by_first_known_requester_2).alias("first_known_requester_id_rank"),
        F.sum("csat_avg_3_prior").over(w_priority_or_unknown_prev3_cs).alias("priority_or_unknown_prev3_csat_sum_csat_avg_3_pr"),
        F.ntile(5).over(w_requester_id_dense_rank_by_s).alias("second_requester_id_quintile"),
        F.sum("last_known_csat").over(w_priority_or_unknown_prev3_cs).alias("priority_or_unknown_prev3_csat_sum_last_known_cs"),
        F.dense_rank().over(w_channel_csat_by_second_reque).alias("second_requester_id_dense_rank"),
    )
)

result = (
    windowed
    .filter(F.col("csat_dense_rank") < 6)
    .select(
        "id",
        "closed_at",
        "csat",
        "channel_prefix",
        "first_known_requester_id",
        "first_known_closed_at",
        "running_count",
        "csat_dense_rank",
        "requester_id_vs_csat_agent_id_avg",
        "channel_min_csat",
        "rows_in_channel",
        "distinct_channel_in_csat_agent_id",
        "rn",
        "csat_avg_3_prior_adj",
        "priority_or_unknown",
        "first_known_rows_in_channel",
        "first_known_requester_id_rank",
        F.concat_ws("-", F.col("priority_or_unknown"), F.col("channel_prefix")).alias("priority_or_unknown_channel_prefix_label"),
        F.round(F.col("csat_avg_3_prior_adj") * 1.07, 2).alias("csat_avg_3_prior_adj_adj"),
    )
    .orderBy(F.col("running_count").asc(), F.col("first_known_closed_at").asc())
)
