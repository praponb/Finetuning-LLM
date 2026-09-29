from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_csat = Window.partitionBy(F.month("closed_at"), "csat")
w_closed_at_month_by_requester = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("csat", "id").rowsBetween(-3, Window.currentRow)
w_closed_at_month_by_requester_2 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").desc(), "id")
w_closed_at_month_by_requester_3 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id").desc())
w_channel_closed_at_month_by_c_2 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("csat", "id")
w_csat_agent_id_by_csat_ties = Window.partitionBy("csat", "agent_id").orderBy("csat")
w_priority_by_closed_at_runnin = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_closed_at_year_c = Window.partitionBy(F.year("first_known_closed_at"), "csat").orderBy(F.col("closed_at_month_csat_sum_requester_id").desc())
w_all_by_csat_percent_rank = Window.orderBy(F.col("csat_percent_rank").desc(), "id")
w_all_by_csat_percent_rank_tie = Window.orderBy(F.col("csat_percent_rank").desc())
w_agent_id_requester_id_rank = Window.partitionBy("agent_id", "requester_id_rank")

filtered_tickets = df_tickets.filter(F.col("agent_id") <= 7)

with_windows = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_month_csat), F.lit(0)), 4).alias("requester_id_share_of_closed_at_month_csat"),
        F.max("requester_id").over(w_closed_at_month_by_requester).alias("max_requester_id_so_far"),
        F.max("requester_id").over(w_channel_closed_at_month_by_c).alias("requester_id_max_4"),
        (F.col("requester_id") - F.lag("requester_id").over(w_closed_at_month_by_requester_2)).alias("requester_id_change"),
        F.rank().over(w_closed_at_month_by_requester_3).alias("requester_id_rank"),
        F.size(F.collect_set("agent_id").over(w_closed_at_month_csat)).alias("distinct_agent_id_in_closed_at_month_csat"),
        F.first("closed_at", ignorenulls=True).over(w_closed_at_month_by_requester).alias("first_known_closed_at"),
        F.round(F.cume_dist().over(w_closed_at_month_by_requester_3), 3).alias("requester_id_cume_dist"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month_csat), 2).alias("requester_id_vs_closed_at_month_csat_avg"),
        F.sum("requester_id").over(w_closed_at_month_csat).alias("closed_at_month_csat_sum_requester_id"),
        F.dense_rank().over(w_channel_closed_at_month_by_c_2).alias("csat_dense_rank"),
        F.round(F.percent_rank().over(w_csat_agent_id_by_csat_ties), 3).alias("csat_percent_rank"),
        F.first("requester_id", ignorenulls=True).over(w_priority_by_closed_at_runnin).alias("first_known_requester_id"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.round(F.percent_rank().over(w_first_known_closed_at_year_c), 3).alias("closed_at_month_csat_sum_requester_id_percent_ra"),
        F.lag("csat", 1, 0).over(w_all_by_csat_percent_rank).alias("prev_csat"),
        F.round(F.percent_rank().over(w_all_by_csat_percent_rank_tie), 3).alias("csat_percent_rank_percent_rank"),
        F.round(F.col("requester_id_max_4") - F.avg("requester_id_max_4").over(w_agent_id_requester_id_rank), 2).alias("requester_id_max_4_vs_agent_id_requester_id_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        "max_requester_id_so_far",
        "requester_id_max_4",
        "requester_id_change",
        "requester_id_rank",
        "first_known_closed_at",
        "requester_id_vs_closed_at_month_csat_avg",
        "closed_at_month_csat_sum_requester_id",
        "csat_dense_rank",
        "csat_percent_rank",
        "closed_at_month_csat_sum_requester_id_percent_ra",
        "prev_csat",
        "requester_id_max_4_vs_agent_id_requester_id_rank",
        F.round(F.col("requester_id_cume_dist") * 0.25, 2).alias("requester_id_cume_dist_adj"),
    )
    .orderBy(
        F.col("closed_at_month_csat_sum_requester_id").asc(),
        F.col("first_known_closed_at").desc(),
    )
)
