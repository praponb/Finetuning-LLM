from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_priority_by_csat_moving = Window.partitionBy("csat", "priority").orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_opened_at_to_date_closed_at = Window.partitionBy(F.to_date("opened_at"), F.year("closed_at")).orderBy("opened_at")
w_csat = Window.partitionBy("csat")
w_all_by_closed_at_running = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_to_date_closed_at_2 = Window.partitionBy(F.to_date("opened_at"), F.year("closed_at")).orderBy("opened_at", "id")
w_opened_at_to_date_closed_at_3 = Window.partitionBy(F.to_date("opened_at"), F.year("closed_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel = Window.partitionBy("channel")
w_agent_id_channel_by_closed_a = Window.partitionBy("agent_id", "channel").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_channel_by_closed_a_2 = Window.partitionBy("agent_id", "channel").orderBy(F.col("closed_at").desc(), "id")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_all_by_total_requester_id_sh = Window.orderBy(F.col("total_requester_id_share_of_channel").asc_nulls_last(), "first_csat", "agent_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_total_requester_id_sh_2 = Window.orderBy(F.col("total_requester_id_share_of_channel").asc_nulls_last(), "first_csat", "agent_id")
w_all_by_total_requester_id_sh_3 = Window.orderBy(F.col("total_requester_id_share_of_channel").asc_nulls_last(), "first_csat", "agent_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_first_csat = Window.partitionBy("distinct_ids", "first_csat")

recent_tickets = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-03-08 23:00:00").cast("timestamp")) & (F.col("opened_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")))
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

sequenced = (
    recent_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        F.round(F.avg("requester_id").over(w_csat_priority_by_csat_moving), 2).alias("requester_id_avg_3"),
        F.dense_rank().over(w_opened_at_to_date_closed_at).alias("opened_at_dense_rank"),
        F.size(F.collect_set("priority").over(w_csat)).alias("distinct_priority_in_csat"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.sum("csat").over(w_all_by_closed_at_running).alias("running_csat"),
        F.lead("closed_at").over(w_opened_at_to_date_closed_at_2).alias("next_closed_at"),
        F.first("csat").over(w_opened_at_to_date_closed_at_3).alias("first_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel), F.lit(0)), 4).alias("requester_id_share_of_channel"),
        F.min("requester_id").over(w_agent_id_channel_by_closed_a).alias("min_requester_id_so_far"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat), 2).alias("csat_vs_csat_avg"),
        F.row_number().over(w_agent_id_channel_by_closed_a_2).alias("seq_no"),
        F.row_number().over(w_all_by_opened_at).alias("seq_no_2"),
        F.lag("opened_at").over(w_agent_id_channel_by_closed_a_2).alias("prev_opened_at"),
    )
)

leaders = (
    sequenced
    .filter((F.col("opened_at_dense_rank") == 1) & (F.col("distinct_priority_in_csat") > 4))
)

per_group = (
    leaders
    .groupBy("first_csat", "agent_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("min_requester_id_so_far").alias("min_min_requester_id_so_far"),
        F.count("*").alias("n_rows"),
        F.sum("requester_id_share_of_channel").alias("total_requester_id_share_of_channel"),
    )
)

sequenced_2 = (
    per_group
    .select(
        "first_csat",
        "agent_id",
        "distinct_ids",
        "min_min_requester_id_so_far",
        F.sum("n_rows").over(w_all_by_total_requester_id_sh).alias("running_n_rows"),
        F.lag("n_rows").over(w_all_by_total_requester_id_sh_2).alias("prev_n_rows"),
        F.nth_value("n_rows", 2).over(w_all_by_total_requester_id_sh_3).alias("second_n_rows"),
        F.round(F.col("min_min_requester_id_so_far") - F.avg("min_min_requester_id_so_far").over(w_distinct_ids_first_csat), 2).alias("min_min_requester_id_so_far_vs_distinct_ids_firs"),
    )
)

result = (
    sequenced_2
    .select(
        "first_csat",
        "distinct_ids",
        "running_n_rows",
        "prev_n_rows",
        "second_n_rows",
        (F.floor(F.col("second_n_rows") / 2) * 2).alias("second_n_rows_bucket"),
        (F.floor(F.col("second_n_rows") / 2) * 2).alias("second_n_rows_bucket_2"),
    )
    .orderBy("second_n_rows_bucket_2", F.col("prev_n_rows").desc())
)
