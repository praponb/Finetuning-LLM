from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_csat = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat").desc(), "id")
w_priority_agent_id_by_csat_ru = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_channel = Window.partitionBy("channel")
w_priority_agent_id_by_csat_fu = Window.partitionBy("priority", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_2 = Window.orderBy("csat", "id")
w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())
w_all_by_csat_moving_2 = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(-2, Window.currentRow)
w_prev3_closed_at_to_date_by_p = Window.partitionBy(F.to_date("prev3_closed_at")).orderBy("prev3_closed_at", "id")
w_priority_by_csat_share_of_ag = Window.partitionBy("priority").orderBy(F.col("csat_share_of_agent_id").desc(), "id")
w_prev3_closed_at_to_date = Window.partitionBy(F.to_date("prev3_closed_at"))
w_last_csat_by_seq_no_running = Window.partitionBy("last_csat").orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat").between(2, 4)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_trends = (
    scoped_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.row_number().over(w_priority_agent_id_by_csat).alias("seq_no"),
        F.sum("csat").over(w_priority_agent_id_by_csat_ru).alias("running_csat"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.cume_dist().over(w_all_by_csat), 3).alias("csat_cume_dist"),
        (F.col("csat") - F.lag("csat").over(w_all_by_csat)).alias("csat_change"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.last("csat").over(w_priority_agent_id_by_csat_fu).alias("last_csat"),
        F.lag("closed_at", 3).over(w_all_by_csat).alias("prev3_closed_at"),
        F.dense_rank().over(w_all_by_csat_2).alias("csat_dense_rank"),
        F.first("closed_at", ignorenulls=True).over(w_priority_agent_id_by_csat_ru).alias("first_known_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id), F.lit(0)), 4).alias("csat_share_of_agent_id"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.round(F.avg("requester_id").over(w_all_by_csat_moving_2), 2).alias("requester_id_avg_3"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("csat_dense_rank") == 1)
    .drop("csat_dense_rank")
)

with_windows = (
    top_rows
    .select(
        "*",
        (F.col("csat_share_of_agent_id") - F.lag("csat_share_of_agent_id").over(w_prev3_closed_at_to_date_by_p)).alias("csat_share_of_agent_id_change"),
        F.lead("requester_id_vs_channel_avg", 3).over(w_priority_by_csat_share_of_ag).alias("next3_requester_id_vs_channel_avg"),
        F.min("seq_no").over(w_prev3_closed_at_to_date).alias("prev3_closed_at_to_date_min_seq_no"),
        F.lag("prev3_closed_at", 3).over(w_prev3_closed_at_to_date_by_p).alias("prev3_prev3_closed_at"),
        F.last("csat_percent_rank", ignorenulls=True).over(w_last_csat_by_seq_no_running).alias("last_known_csat_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "seq_no",
        "running_csat",
        "rows_in_agent_id",
        "csat_cume_dist",
        "csat_change",
        "last_csat",
        "prev3_closed_at",
        "first_known_closed_at",
        "csat_share_of_agent_id",
        "csat_percent_rank",
        "requester_id_avg_3",
        "csat_share_of_agent_id_change",
        "next3_requester_id_vs_channel_avg",
        "prev3_closed_at_to_date_min_seq_no",
        "prev3_prev3_closed_at",
        "last_known_csat_percent_rank",
    )
)
