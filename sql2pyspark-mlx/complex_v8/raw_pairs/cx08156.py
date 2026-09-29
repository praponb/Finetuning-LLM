from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_moving_2 = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_priority_by_requester_id = Window.partitionBy("priority").orderBy("requester_id", "id")
w_csat_closed_at_to_date_by_cs = Window.partitionBy("csat", F.to_date("closed_at")).orderBy("csat", "id")
w_priority = Window.partitionBy("priority")
w_csat = Window.partitionBy("csat")
w_priority_by_requester_id_run = Window.partitionBy("priority").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_requester_id_ful = Window.partitionBy("priority").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_closed_at_month_by = Window.partitionBy("priority", F.month("closed_at")).orderBy("requester_id")
w_priority_closed_at_month_by_2 = Window.partitionBy("priority", F.month("closed_at")).orderBy("requester_id", "id")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())

cleaned_tickets = df_tickets.filter((F.col("requester_id") <= 23) & (F.col("channel") != "web"))

with_windows = (
    cleaned_tickets
    .select(
        "*",
        F.max("csat").over(w_all_by_opened_at_moving_2).alias("csat_max_3"),
        F.rank().over(w_priority_by_requester_id).alias("requester_id_rank"),
        F.lead("closed_at", 3).over(w_csat_closed_at_to_date_by_cs).alias("next3_closed_at"),
        F.lag("csat").over(w_csat_closed_at_to_date_by_cs).alias("prev_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
        F.min("csat").over(w_priority_by_requester_id_run).alias("min_csat_so_far"),
        F.nth_value("closed_at", 2).over(w_priority_by_requester_id_ful).alias("second_closed_at"),
        F.dense_rank().over(w_priority_closed_at_month_by).alias("requester_id_dense_rank"),
        F.lag("opened_at", 3).over(w_priority_closed_at_month_by_2).alias("prev3_opened_at"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "requester_id_rank",
        "next3_closed_at",
        "csat_share_of_priority",
        "csat_sum_csat",
        "min_csat_so_far",
        "requester_id_dense_rank",
        "opened_at_dense_rank",
    )
    .orderBy(F.col("requester_id_dense_rank").desc_nulls_first(), "id")
    .limit(10)
)
