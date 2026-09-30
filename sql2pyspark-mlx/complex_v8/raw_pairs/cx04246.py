from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_to_date_priority = Window.partitionBy(F.to_date("closed_at"), "priority")
w_agent_id_priority_by_csat = Window.partitionBy("agent_id", "priority").orderBy(F.col("csat").desc(), "id")
w_csat = Window.partitionBy("csat")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_agent_id_priority_by_csat_ti = Window.partitionBy("agent_id", "priority").orderBy(F.col("csat").desc())
w_csat_by_closed_at = Window.partitionBy("csat").orderBy("closed_at", "id")
w_csat_opened_at_year_by_csat = Window.partitionBy("csat", F.year("opened_at")).orderBy("csat")
w_agent_id_priority_by_csat_pr = Window.partitionBy("agent_id", "priority").orderBy(F.col("csat").desc(), "id").rowsBetween(-2, -1)
w_all_by_csat_rank = Window.orderBy("csat_rank", "id")
w_all_by_csat_rank_full = Window.orderBy("csat_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_rank_2 = Window.partitionBy("csat_rank_2")
w_priority_csat_rank_2_by_requ = Window.partitionBy("priority", "csat_rank_2").orderBy("requester_id", "id")
w_closed_at_dense_rank_by_requ = Window.partitionBy("closed_at_dense_rank").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, -1)

scoped = df_tickets.filter(F.col("channel") == "phone")

metrics = (
    scoped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        F.max("requester_id").over(w_closed_at_to_date_priority).alias("closed_at_to_date_priority_max_requester_id"),
        F.dense_rank().over(w_agent_id_priority_by_csat).alias("csat_dense_rank"),
        F.max("requester_id").over(w_csat).alias("csat_max_requester_id"),
        F.dense_rank().over(w_all_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.rank().over(w_agent_id_priority_by_csat_ti).alias("csat_rank"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat), 2).alias("requester_id_vs_csat_avg"),
        F.row_number().over(w_csat_by_closed_at).alias("closed_at_seq"),
        F.rank().over(w_csat_opened_at_year_by_csat).alias("csat_rank_2"),
        F.round(F.avg("requester_id").over(w_agent_id_priority_by_csat_pr), 2).alias("requester_id_avg_2_prior"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "priority",
        "closed_at_to_date_priority_max_requester_id",
        "csat_max_requester_id",
        "closed_at_dense_rank",
        "csat_rank",
        "requester_id_vs_csat_avg",
        "csat_rank_2",
        "requester_id_avg_2_prior",
        F.ntile(3).over(w_all_by_csat_rank).alias("csat_rank_tercile"),
        F.nth_value("requester_id", 2).over(w_all_by_csat_rank_full).alias("second_requester_id"),
        F.size(F.collect_set("priority").over(w_csat_rank_2)).alias("distinct_priority_in_csat_rank_2"),
        F.ntile(3).over(w_priority_csat_rank_2_by_requ).alias("requester_id_tercile"),
        F.round(F.percent_rank().over(w_all_by_csat_rank), 3).alias("csat_rank_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_csat_rank), 3).alias("csat_rank_cume_dist"),
        F.sum("requester_id_avg_2_prior").over(w_closed_at_dense_rank_by_requ).alias("requester_id_avg_2_prior_sum_3_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "priority",
        "closed_at_to_date_priority_max_requester_id",
        "csat_max_requester_id",
        "csat_rank",
        "requester_id_vs_csat_avg",
        "csat_rank_2",
        "requester_id_avg_2_prior",
        "csat_rank_tercile",
        "second_requester_id",
        "distinct_priority_in_csat_rank_2",
        "requester_id_tercile",
        "csat_rank_percent_rank",
        "requester_id_avg_2_prior_sum_3_prior",
        F.when(F.col("csat_max_requester_id") >= 188, "HIGH").when(F.col("csat_max_requester_id") >= 63, "MEDIUM").otherwise("LOW").alias("csat_max_requester_id_tier"),
        (F.floor(F.col("requester_id_vs_csat_avg") / 10) * 10).alias("requester_id_vs_csat_avg_bucket"),
    )
    .orderBy(F.col("requester_id_avg_2_prior").asc_nulls_last(), "id")
    .limit(10)
)
