from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id_full = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_prior_6 = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(-6, -1)
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_channel = Window.partitionBy("channel")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_all_by_opened_at_2 = Window.orderBy("opened_at", "id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_2 = Window.orderBy(F.col("requester_id").desc(), "id")
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_requester_id_rank = Window.partitionBy("priority", "requester_id_rank")
w_next_opened_at_year_by_seq_n = Window.partitionBy(F.year("next_opened_at")).orderBy("seq_no", "id")
w_next3_opened_at_month_priori = Window.partitionBy(F.month("next3_opened_at"), "priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next3_opened_at_month_priori_2 = Window.partitionBy(F.month("next3_opened_at"), "priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_opened_at_year_by_seq_n_2 = Window.partitionBy(F.year("next_opened_at")).orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(10, 16))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.first("requester_id").over(w_all_by_requester_id_full).alias("first_requester_id"),
        F.lead("opened_at", 3).over(w_all_by_requester_id).alias("next3_opened_at"),
        F.sum("requester_id").over(w_all_by_requester_id_prior_6).alias("requester_id_sum_6_prior"),
        F.round(F.cume_dist().over(w_all_by_requester_id_ties), 3).alias("requester_id_cume_dist"),
        F.max("requester_id").over(w_channel).alias("channel_max_requester_id"),
        F.rank().over(w_all_by_requester_id_ties).alias("requester_id_rank"),
        F.row_number().over(w_all_by_requester_id).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.lead("opened_at").over(w_all_by_opened_at_2).alias("next_opened_at"),
        F.row_number().over(w_all_by_opened_at).alias("opened_at_seq"),
        F.last("requester_id").over(w_all_by_opened_at_full).alias("last_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_requester_id_2)).alias("requester_id_change"),
        F.max("requester_id").over(w_all_by_requester_id_running).alias("max_requester_id_so_far"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.count(F.lit(1)).over(w_priority_requester_id_rank).alias("rows_in_priority_requester_id_rank"),
        F.lag("requester_id_sum_6_prior", 2).over(w_next_opened_at_year_by_seq_n).alias("prev2_requester_id_sum_6_prior"),
        F.first("requester_id_change", ignorenulls=True).over(w_next3_opened_at_month_priori).alias("first_known_requester_id_change"),
        F.last("opened_at").over(w_next3_opened_at_month_priori_2).alias("last_opened_at"),
        F.dense_rank().over(w_next_opened_at_year_by_seq_n).alias("seq_no_dense_rank"),
        F.round(F.cume_dist().over(w_next_opened_at_year_by_seq_n), 3).alias("seq_no_cume_dist"),
        F.first("channel_max_requester_id", ignorenulls=True).over(w_next_opened_at_year_by_seq_n_2).alias("first_known_channel_max_requester_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "priority_clean",
        "next3_opened_at",
        "channel_max_requester_id",
        "requester_id_rank",
        "seq_no",
        "opened_at_cume_dist",
        "opened_at_seq",
        "last_requester_id",
        "requester_id_change",
        "rows_in_priority_requester_id_rank",
        "prev2_requester_id_sum_6_prior",
        "first_known_requester_id_change",
        "last_opened_at",
        "seq_no_cume_dist",
        "first_known_channel_max_requester_id",
    )
    .orderBy(F.col("opened_at_seq").desc())
)
