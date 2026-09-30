from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_closed_at_running = Window.partitionBy("csat").orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_closed_at_month = Window.partitionBy(F.month("closed_at"))
w_all_by_csat_full = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_csat_by_closed_at = Window.partitionBy("csat").orderBy(F.col("closed_at").asc_nulls_last(), "id")
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy("csat")
w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())

scoped_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.month(F.col("closed_at")).isin(1, 2)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
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
        "closed_at",
        "csat",
        "tags",
        F.min("csat").over(w_csat_by_closed_at_running).alias("min_csat_so_far"),
        F.sum("requester_id").over(w_csat).alias("csat_sum_requester_id"),
        F.size(F.collect_set("agent_id").over(w_closed_at_month)).alias("distinct_agent_id_in_closed_at_month"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat), 2).alias("requester_id_vs_csat_avg"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.last("requester_id").over(w_all_by_csat_full).alias("last_requester_id"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_csat)).alias("requester_id_change"),
        F.sum("requester_id").over(w_closed_at_month).alias("closed_at_month_sum_requester_id"),
        F.lag("csat").over(w_agent_id_csat_by_csat).alias("prev_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_month), F.lit(0)), 4).alias("csat_share_of_closed_at_month"),
        F.round(F.cume_dist().over(w_csat_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.round(F.percent_rank().over(w_agent_id_csat_by_csat_ties), 3).alias("csat_percent_rank"),
        F.ntile(5).over(w_agent_id_csat_by_csat).alias("csat_quintile"),
        F.rank().over(w_all_by_csat_ties).alias("csat_rank"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month), 2).alias("requester_id_vs_closed_at_month_avg"),
        F.count(F.lit(1)).over(w_closed_at_month).alias("rows_in_closed_at_month"),
    )
)

summary = (
    with_trends
    .groupBy("distinct_agent_id_in_closed_at_month", "prev_csat")
    .agg(
        F.max("prev_csat").alias("max_prev_csat"),
        F.min("closed_at").alias("first_closed_at"),
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("csat_share_of_closed_at_month"), 2).alias("stddev_csat_share_of_closed_at_month"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .filter(F.col("first_closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp"))
    .select(
        "distinct_agent_id_in_closed_at_month",
        "prev_csat",
        "max_prev_csat",
        "first_closed_at",
        "last_closed_at",
        "stddev_csat_share_of_closed_at_month",
    )
    .orderBy(
        F.col("distinct_agent_id_in_closed_at_month").asc_nulls_last(),
        "prev_csat",
    )
    .limit(10)
)
