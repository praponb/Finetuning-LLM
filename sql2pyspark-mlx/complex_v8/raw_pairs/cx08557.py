from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_opened_at_to_date_priority_b = Window.partitionBy(F.to_date("opened_at"), "priority").orderBy("requester_id", "id")
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc_nulls_first())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_priority_by_request = Window.partitionBy("agent_id", "priority").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())
w_priority_by_opened_at_moving = Window.partitionBy("priority").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_full = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at_percent_ran = Window.orderBy(F.col("opened_at_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_opened_at_to_dat = Window.partitionBy(F.to_date("first_known_opened_at"), F.year("first_known_opened_at_2"))
w_all_by_opened_at_percent_ran_2 = Window.orderBy(F.col("opened_at_percent_rank").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_all_by_opened_at_percent_ran_3 = Window.orderBy(F.col("opened_at_percent_rank").asc_nulls_last())
w_all_by_opened_at_percent_ran_4 = Window.orderBy(F.col("opened_at_percent_rank").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_distinct_agent_id_in_priorit = Window.partitionBy("distinct_agent_id_in_priority", "requester_id_dense_rank")

valid_tickets = (
    df_tickets
    .filter((F.col("priority").isNotNull()) & (F.col("requester_id") <= 22))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

sequenced = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        "priority_prefix",
        F.round(F.avg("requester_id").over(w_agent_id_priority), 2).alias("agent_id_priority_avg_requester_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_to_date_priority_b))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.lag("requester_id").over(w_opened_at_to_date_priority_b).alias("prev_requester_id"),
        F.first("opened_at", ignorenulls=True).over(w_priority_by_opened_at_runnin).alias("first_known_opened_at"),
        F.nth_value("requester_id", 2).over(w_all_by_opened_at_full).alias("second_requester_id"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.round(F.percent_rank().over(w_all_by_opened_at_ties), 3).alias("opened_at_percent_rank"),
        F.lead("requester_id").over(w_all_by_opened_at).alias("next_requester_id"),
        F.first("opened_at", ignorenulls=True).over(w_agent_id_priority_by_request).alias("first_known_opened_at_2"),
        F.dense_rank().over(w_all_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.round(F.avg("requester_id").over(w_priority_by_opened_at_moving), 2).alias("requester_id_avg_2"),
        F.min("requester_id").over(w_all_by_opened_at_running).alias("min_requester_id_so_far"),
        F.nth_value("requester_id", 2).over(w_all_by_requester_id_full).alias("second_requester_id_2"),
    )
)

first_rows = sequenced.filter(F.col("requester_id_dense_rank") <= 5)

sequenced_2 = (
    first_rows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "priority_prefix",
        "agent_id_priority_avg_requester_id",
        "minutes_since_prev_opened_at",
        "prev_requester_id",
        "first_known_opened_at",
        "second_requester_id",
        "first_known_opened_at_2",
        "requester_id_dense_rank",
        "min_requester_id_so_far",
        "second_requester_id_2",
        F.count(F.lit(1)).over(w_all_by_opened_at_percent_ran).alias("running_count"),
        F.sum("second_requester_id_2").over(w_first_known_opened_at_to_dat).alias("first_known_opened_at_to_date_first_known_opened"),
        F.round(F.avg("opened_at_percent_rank").over(w_all_by_opened_at_percent_ran_2), 2).alias("opened_at_percent_rank_avg_6_prior"),
        F.rank().over(w_all_by_opened_at_percent_ran_3).alias("opened_at_percent_rank_rank"),
        F.max("min_requester_id_so_far").over(w_all_by_opened_at_percent_ran_4).alias("min_requester_id_so_far_max_2_prior"),
        F.size(F.collect_set("agent_id").over(w_distinct_agent_id_in_priorit)).alias("distinct_agent_id_in_distinct_agent_id_in_priori"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "priority_prefix",
        "agent_id_priority_avg_requester_id",
        "minutes_since_prev_opened_at",
        "first_known_opened_at_2",
        "requester_id_dense_rank",
        "min_requester_id_so_far",
        "second_requester_id_2",
        "running_count",
        "first_known_opened_at_to_date_first_known_opened",
        "opened_at_percent_rank_avg_6_prior",
        "opened_at_percent_rank_rank",
        "min_requester_id_so_far_max_2_prior",
        "distinct_agent_id_in_distinct_agent_id_in_priori",
        F.lower(F.regexp_replace(F.trim(F.col("priority_prefix")), " ", "_")).alias("priority_prefix_slug"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
)
