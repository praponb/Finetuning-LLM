from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_closed_at_f = Window.partitionBy("agent_id", "csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel = Window.partitionBy("channel")
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_csat_by_closed_at = Window.partitionBy("agent_id", "csat").orderBy(F.col("closed_at").desc(), "id")
w_priority_by_requester_id_mov = Window.partitionBy("priority").orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)
w_closed_at_year_channel_by_re = Window.partitionBy(F.year("closed_at"), "channel").orderBy("requester_id", "id")
w_closed_at_year = Window.partitionBy(F.year("closed_at"))
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")

cleaned = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) | (F.col("requester_id") >= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.nth_value("opened_at", 2).over(w_agent_id_csat_by_closed_at_f).alias("second_opened_at"),
        F.max("csat").over(w_channel).alias("channel_max_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.first("requester_id").over(w_agent_id_csat_by_closed_at_f).alias("first_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_agent_id_csat_by_closed_at)).alias("csat_change"),
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.sum("requester_id").over(w_priority_by_requester_id_mov).alias("requester_id_sum_2"),
        F.row_number().over(w_agent_id_csat_by_closed_at).alias("seq_no"),
        F.round(F.avg("requester_id").over(w_channel), 2).alias("channel_avg_requester_id"),
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.round(F.cume_dist().over(w_closed_at_year_channel_by_re), 3).alias("requester_id_cume_dist"),
        F.lead("requester_id").over(w_closed_at_year_channel_by_re).alias("next_requester_id"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.sum("channel_avg_requester_id").over(w_closed_at_year).alias("closed_at_year_sum_channel_avg_requester_id"),
        F.min("seq_no").over(w_closed_at_year).alias("closed_at_year_min_seq_no"),
        F.size(F.collect_set("channel").over(w_closed_at_year)).alias("distinct_channel_in_closed_at_year"),
        F.last("next_requester_id").over(w_all_by_opened_at_full).alias("last_next_requester_id"),
        F.rank().over(w_all_by_opened_at_ties).alias("opened_at_rank"),
        F.lag("seq_no", 3, 0).over(w_all_by_opened_at).alias("prev3_seq_no"),
        F.nth_value("csat_change", 2).over(w_all_by_opened_at_full).alias("second_csat_change"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "second_opened_at",
        "channel_max_csat",
        "requester_id_share_of_agent_id",
        "rows_in_agent_id",
        "first_requester_id",
        "csat_vs_channel_avg",
        "requester_id_sum_2",
        "seq_no",
        "channel_avg_requester_id",
        "next_requester_id",
        "closed_at_year_sum_channel_avg_requester_id",
        "closed_at_year_min_seq_no",
        "distinct_channel_in_closed_at_year",
        "last_next_requester_id",
        "opened_at_rank",
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        (F.col("opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("opened_at_plus_15m"),
    )
)
