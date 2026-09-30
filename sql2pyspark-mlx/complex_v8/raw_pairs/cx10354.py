from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_n_tier_is_not_null_by_n = Window.partitionBy("tier", "n_tier_is_not_null").orderBy("n_tier_is_not_null", "tier", "hired_on_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_tier_is_not_null_tier_by_n = Window.partitionBy("n_tier_is_not_null", "tier").orderBy(F.col("n_tier_is_not_null").desc(), "tier", "hired_on_ym")
w_tier_n_tier_is_not_null_by_n_2 = Window.partitionBy("tier", "n_tier_is_not_null").orderBy("n_tier_is_not_null")
w_n_tier_is_not_null = Window.partitionBy("n_tier_is_not_null")
w_tier_n_tier_is_not_null_by_n_3 = Window.partitionBy("tier", "n_tier_is_not_null").orderBy("n_tier_is_not_null", "tier", "hired_on_ym")
w_n_tier_is_not_null_tier = Window.partitionBy("n_tier_is_not_null", "tier")
w_n_tier_is_not_null_distinct = Window.partitionBy("n_tier_is_not_null", "distinct_tier_in_n_tier_is_not_null").orderBy("distinct_tier_in_n_tier_is_not_null", "tier", "hired_on_ym").rowsBetween(-3, -1)
w_n_tier_is_not_null_distinct_2 = Window.partitionBy("n_tier_is_not_null", "distinct_tier_in_n_tier_is_not_null").orderBy("distinct_tier_in_n_tier_is_not_null", "tier", "hired_on_ym")

recent = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-27").cast("date"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("requester_id").alias("total_ticket_requester_id"),
        F.count(F.when(F.col("priority") == "P3", 1)).alias("ticket_priority_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.total_ticket_requester_id",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

stats = (
    summarized_rows
    .filter(F.lower(F.col("agent_name")).like("%au%"))
    .groupBy("tier", F.date_format("hired_on", "yyyy-MM").alias("hired_on_ym"))
    .agg(
        F.sum(F.when(F.col("tier").isNotNull(), 1).otherwise(0)).alias("n_tier_is_not_null"),
        F.round(F.stddev("total_ticket_requester_id"), 2).alias("stddev_total_ticket_requester_id"),
        F.sum("ticket_priority_count").alias("sum_ticket_priority_count_check"),
    )
    .filter(F.col("sum_ticket_priority_count_check") > 2)
    .drop("sum_ticket_priority_count_check")
)

sequenced = (
    stats
    .select(
        "tier",
        "hired_on_ym",
        "n_tier_is_not_null",
        "stddev_total_ticket_requester_id",
        F.first("n_tier_is_not_null").over(w_tier_n_tier_is_not_null_by_n).alias("first_n_tier_is_not_null"),
        (F.col("n_tier_is_not_null") - F.lag("n_tier_is_not_null").over(w_n_tier_is_not_null_tier_by_n)).alias("n_tier_is_not_null_change"),
        F.round(F.percent_rank().over(w_tier_n_tier_is_not_null_by_n_2), 3).alias("n_tier_is_not_null_percent_rank"),
        F.size(F.collect_set("tier").over(w_n_tier_is_not_null)).alias("distinct_tier_in_n_tier_is_not_null"),
        F.round(F.cume_dist().over(w_tier_n_tier_is_not_null_by_n_2), 3).alias("n_tier_is_not_null_cume_dist"),
        F.ntile(4).over(w_tier_n_tier_is_not_null_by_n_3).alias("n_tier_is_not_null_quartile"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.round(F.col("n_tier_is_not_null") - F.avg("n_tier_is_not_null").over(w_n_tier_is_not_null_tier), 2).alias("n_tier_is_not_null_vs_n_tier_is_not_null_tier_av"),
        F.round(F.avg("first_n_tier_is_not_null").over(w_n_tier_is_not_null_tier), 2).alias("n_tier_is_not_null_tier_avg_first_n_tier_is_not_"),
        F.round(F.avg("first_n_tier_is_not_null").over(w_n_tier_is_not_null_distinct), 2).alias("first_n_tier_is_not_null_avg_3_prior"),
        (F.col("n_tier_is_not_null") - F.lag("n_tier_is_not_null").over(w_n_tier_is_not_null_distinct_2)).alias("n_tier_is_not_null_change_2"),
        F.count(F.lit(1)).over(w_n_tier_is_not_null_tier).alias("rows_in_n_tier_is_not_null_tier"),
    )
)

result = (
    with_windows
    .filter(F.col("tier") != "T2")
    .select(
        "tier",
        "n_tier_is_not_null",
        "stddev_total_ticket_requester_id",
        "first_n_tier_is_not_null",
        "n_tier_is_not_null_change",
        "n_tier_is_not_null_percent_rank",
        "n_tier_is_not_null_cume_dist",
        "n_tier_is_not_null_quartile",
        "n_tier_is_not_null_vs_n_tier_is_not_null_tier_av",
        "first_n_tier_is_not_null_avg_3_prior",
    )
)
