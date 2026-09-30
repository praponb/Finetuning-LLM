from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_closed_at_to_date_by_cs = Window.partitionBy("csat", F.to_date("closed_at")).orderBy(F.col("csat").asc_nulls_last(), "id")
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_csat_by_closed_at = Window.partitionBy("priority", "csat").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_csat = Window.partitionBy("csat")
w_priority_closed_at_month_by = Window.partitionBy("priority", F.month("closed_at")).orderBy(F.col("csat").desc(), "id")
w_agent_id_by_closed_at_ties = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc())
w_csat_priority_by_csat_per_re = Window.partitionBy("csat", "priority").orderBy("csat_per_requester_id")
w_all_by_csat_per_requester_id = Window.orderBy(F.col("csat_per_requester_id_vs_agent_id_priority_avg").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_csat_per_requester_id_2 = Window.orderBy(F.col("csat_per_requester_id_vs_agent_id_priority_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_closed_at_running = Window.partitionBy("csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_distinct_agent_id_i = Window.partitionBy("priority", "distinct_agent_id_in_csat")

cleaned_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

windowed = (
    cleaned_tickets
    .select(
        "*",
        F.round(F.cume_dist().over(w_csat_closed_at_to_date_by_cs), 3).alias("csat_cume_dist"),
        F.sum("csat").over(w_agent_id_by_closed_at_runnin).alias("running_csat"),
        F.round(F.cume_dist().over(w_priority_csat_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.round(F.col("csat_per_requester_id") - F.avg("csat_per_requester_id").over(w_agent_id_priority), 2).alias("csat_per_requester_id_vs_agent_id_priority_avg"),
        F.lead("csat_per_requester_id", 3).over(w_priority_closed_at_month_by).alias("next3_csat_per_requester_id"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.dense_rank().over(w_agent_id_by_closed_at_ties).alias("closed_at_dense_rank"),
        (F.col("csat_per_requester_id") - F.lag("csat_per_requester_id").over(w_priority_csat_by_closed_at)).alias("csat_per_requester_id_change"),
        F.min("csat").over(w_agent_id_priority).alias("agent_id_priority_min_csat"),
        F.round(F.cume_dist().over(w_csat_priority_by_csat_per_re), 3).alias("csat_per_requester_id_cume_dist"),
    )
)

leaders = (
    windowed
    .filter(F.col("closed_at_dense_rank") == 1)
    .drop("closed_at_dense_rank")
)

sequenced = (
    leaders
    .select(
        "*",
        F.max("csat").over(w_all_by_csat_per_requester_id).alias("csat_max_2_prior"),
        F.nth_value("csat_share_of_csat", 2).over(w_all_by_csat_per_requester_id_2).alias("second_csat_share_of_csat"),
        F.sum("rows_in_csat").over(w_csat_by_closed_at_running).alias("running_rows_in_csat"),
        F.sum("agent_id_priority_min_csat").over(w_priority_distinct_agent_id_i).alias("priority_distinct_agent_id_in_csat_sum_agent_id_"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        "csat_per_requester_id",
        "running_csat",
        "closed_at_cume_dist",
        "csat_per_requester_id_vs_agent_id_priority_avg",
        "next3_csat_per_requester_id",
        "distinct_agent_id_in_csat",
        "csat_share_of_csat",
        "csat_per_requester_id_change",
        "csat_per_requester_id_cume_dist",
        "priority_distinct_agent_id_in_csat_sum_agent_id_",
    )
    .orderBy(
        F.col("distinct_agent_id_in_csat").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
