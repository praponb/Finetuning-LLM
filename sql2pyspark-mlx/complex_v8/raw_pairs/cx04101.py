from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_month = Window.partitionBy("agent_id", F.month("opened_at"))
w_closed_at_month = Window.partitionBy(F.date_trunc("month", "closed_at"))
w_opened_at_month_csat_by_requ = Window.partitionBy(F.date_trunc("month", "opened_at"), "csat").orderBy("requester_id")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_csat_agent_id_by_closed_at_t = Window.partitionBy("csat", "agent_id").orderBy("closed_at")
w_opened_at_month_csat_by_requ_2 = Window.partitionBy(F.date_trunc("month", "opened_at"), "csat").orderBy("requester_id", "id")
w_all_by_opened_at_2 = Window.orderBy(F.col("opened_at").desc(), "id")
w_opened_at_month_csat_by_requ_3 = Window.partitionBy(F.date_trunc("month", "opened_at"), "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_csat_last_opened_at_2_t = Window.partitionBy("prev_csat", F.to_date("last_opened_at_2")).orderBy(F.col("last_opened_at_2").desc_nulls_first(), "id")
w_prev_csat_last_opened_at_2_t_2 = Window.partitionBy("prev_csat", F.to_date("last_opened_at_2")).orderBy(F.col("last_opened_at_2").desc_nulls_first())
w_prev_csat_last_opened_at_mon = Window.partitionBy("prev_csat", F.date_trunc("month", "last_opened_at")).orderBy("requester_id")
w_opened_at_rank_agent_id = Window.partitionBy("opened_at_rank", "agent_id")

recent = (
    df_tickets
    .filter(F.col("requester_id") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_trends = (
    recent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.sum("requester_id").over(w_agent_id_opened_at_month).alias("agent_id_opened_at_month_sum_requester_id"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month), 2).alias("requester_id_vs_closed_at_month_avg"),
        F.min("csat").over(w_agent_id_opened_at_month).alias("agent_id_opened_at_month_min_csat"),
        F.round(F.percent_rank().over(w_opened_at_month_csat_by_requ), 3).alias("requester_id_percent_rank"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
        F.last("opened_at").over(w_all_by_opened_at_full).alias("last_opened_at"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_opened_at)).alias("requester_id_change"),
        F.max("csat").over(w_all_by_requester_id_running).alias("max_csat_so_far"),
        F.round(F.avg("requester_id").over(w_csat_agent_id), 2).alias("csat_agent_id_avg_requester_id"),
        F.round(F.avg("csat").over(w_agent_id_opened_at_month), 2).alias("agent_id_opened_at_month_avg_csat"),
        F.dense_rank().over(w_csat_agent_id_by_closed_at_t).alias("closed_at_dense_rank"),
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.lag("csat").over(w_opened_at_month_csat_by_requ_2).alias("prev_csat"),
        F.lead("requester_id", 3, 0).over(w_all_by_opened_at_2).alias("next3_requester_id"),
        F.last("opened_at").over(w_opened_at_month_csat_by_requ_3).alias("last_opened_at_2"),
        F.nth_value("closed_at", 2).over(w_opened_at_month_csat_by_requ_3).alias("second_closed_at"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("closed_at_dense_rank") <= 3)
    .drop("closed_at_dense_rank")
)

windowed = (
    top_rows
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "agent_id_opened_at_month_sum_requester_id",
        "requester_id_vs_closed_at_month_avg",
        "agent_id_opened_at_month_min_csat",
        "requester_id_percent_rank",
        "seq_no",
        "last_opened_at",
        "requester_id_change",
        "csat_agent_id_avg_requester_id",
        "opened_at_rank",
        "prev_csat",
        "next3_requester_id",
        "last_opened_at_2",
        F.ntile(4).over(w_prev_csat_last_opened_at_2_t).alias("last_opened_at_2_quartile"),
        F.round(F.cume_dist().over(w_prev_csat_last_opened_at_2_t_2), 3).alias("last_opened_at_2_cume_dist"),
        F.dense_rank().over(w_prev_csat_last_opened_at_mon).alias("requester_id_dense_rank"),
        (F.col("requester_id_change") - F.lag("requester_id_change").over(w_prev_csat_last_opened_at_2_t)).alias("requester_id_change_change"),
        F.max("max_csat_so_far").over(w_opened_at_rank_agent_id).alias("opened_at_rank_agent_id_max_max_csat_so_far"),
        F.count(F.lit(1)).over(w_opened_at_rank_agent_id).alias("rows_in_opened_at_rank_agent_id"),
        F.sum("max_csat_so_far").over(w_opened_at_rank_agent_id).alias("opened_at_rank_agent_id_sum_max_csat_so_far"),
        F.round(F.col("next3_requester_id") - F.avg("next3_requester_id").over(w_opened_at_rank_agent_id), 2).alias("next3_requester_id_vs_opened_at_rank_agent_id_av"),
    )
)

result = (
    windowed
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "agent_id_opened_at_month_sum_requester_id",
        "agent_id_opened_at_month_min_csat",
        "last_opened_at",
        "requester_id_change",
        "csat_agent_id_avg_requester_id",
        "prev_csat",
        "last_opened_at_2",
        "last_opened_at_2_quartile",
        "last_opened_at_2_cume_dist",
        "requester_id_dense_rank",
        "requester_id_change_change",
        "opened_at_rank_agent_id_max_max_csat_so_far",
        "rows_in_opened_at_rank_agent_id",
        "opened_at_rank_agent_id_sum_max_csat_so_far",
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("last_opened_at").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
