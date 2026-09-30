from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_closed_at_prior = Window.partitionBy("priority").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-2, -1)
w_closed_at_to_date_csat = Window.partitionBy(F.to_date("closed_at"), "csat")
w_csat_filled_priority = Window.partitionBy("csat_filled", "priority")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_channel_by_csat_filled = Window.partitionBy("channel").orderBy("csat_filled", "id")
w_closed_at_month_priority_by = Window.partitionBy(F.date_trunc("month", "closed_at"), "priority").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_closed_at = Window.partitionBy("priority").orderBy(F.col("closed_at").desc(), "id")
w_all_by_csat_running = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_csat_filled_ties = Window.partitionBy("channel").orderBy("csat_filled")
w_channel = Window.partitionBy("channel")
w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.month("closed_at")).orderBy(F.col("csat_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) | (F.col("requester_id") < 17))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("channel").isin("web", "phone"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

metrics = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        F.max("csat_filled").over(w_priority_by_closed_at_prior).alias("csat_filled_max_2_prior"),
        F.size(F.collect_set("channel_group").over(w_closed_at_to_date_csat)).alias("distinct_channel_group_in_closed_at_to_date_csat"),
        F.count(F.lit(1)).over(w_csat_filled_priority).alias("rows_in_csat_filled_priority"),
        F.round(F.col("csat_filled") - F.avg("csat_filled").over(w_csat_filled_priority), 2).alias("csat_filled_vs_csat_filled_priority_avg"),
        F.round(F.percent_rank().over(w_all_by_csat), 3).alias("csat_percent_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_channel_by_csat_filled))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_month_priority_by).alias("running_count"),
        F.dense_rank().over(w_priority_by_closed_at).alias("closed_at_dense_rank"),
        F.count(F.lit(1)).over(w_closed_at_to_date_csat).alias("rows_in_closed_at_to_date_csat"),
        F.count(F.lit(1)).over(w_all_by_csat_running).alias("running_count_2"),
        F.rank().over(w_channel_by_csat_filled_ties).alias("csat_filled_rank"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel), F.lit(0)), 4).alias("csat_share_of_channel"),
        F.min("csat_filled").over(w_channel_closed_at_month_by_c).alias("min_csat_filled_so_far"),
    )
)

grouped = (
    metrics
    .filter(F.col("minutes_since_prev_closed_at") < 3250.0)
    .groupBy("closed_at_dense_rank", "csat", "csat_filled_rank")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.sum("rows_in_csat_filled_priority").alias("total_rows_in_csat_filled_priority"),
    )
)

result = (
    grouped
    .select(
        "closed_at_dense_rank",
        "csat",
        "cnt",
        "total_rows_in_csat_filled_priority",
        F.coalesce(F.col("avg_csat"), F.lit(0.0)).alias("avg_csat_filled"),
        F.round(F.col("closed_at_dense_rank") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("closed_at_dense_rank_per_csat"),
    )
    .orderBy(F.col("cnt").desc())
)
