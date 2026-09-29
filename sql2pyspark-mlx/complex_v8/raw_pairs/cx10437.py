from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_opened_at_month_csat_tier_by = Window.partitionBy(F.month("opened_at"), "csat_tier").orderBy("opened_at", "id")
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_moving_2 = Window.orderBy("opened_at", "id").rowsBetween(-2, Window.currentRow)
w_agent_id_csat_by_csat_runnin = Window.partitionBy("agent_id", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_to_date_by_csat_fu = Window.partitionBy(F.to_date("closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_tier = Window.partitionBy("csat_tier")
w_all_by_opened_at_full = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_csat_by_csat_full = Window.partitionBy("agent_id", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_known_closed_at_to_date = Window.partitionBy(F.to_date("last_known_closed_at"), F.month("last_opened_at"))
w_all_by_opened_at_running = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_prior_6 = Window.orderBy("opened_at", "id").rowsBetween(-6, -1)
w_first_closed_at_year_by_last = Window.partitionBy(F.year("first_closed_at")).orderBy(F.col("last_known_closed_at_to_date_last_opened_at_mont_2").desc(), "closed_at_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tickets = (
    df_tickets
    .filter((F.col("csat") >= 2) & (F.col("opened_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

with_trends = (
    base_tickets
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "csat_tier",
        F.row_number().over(w_agent_id_csat_by_csat).alias("rn"),
        (F.col("csat") - F.lag("csat").over(w_all_by_opened_at)).alias("csat_change"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat_agent_id), 2).alias("requester_id_vs_csat_agent_id_avg"),
        F.rank().over(w_opened_at_month_csat_tier_by).alias("opened_at_rank"),
        F.min("requester_id").over(w_priority_by_opened_at_runnin).alias("min_requester_id_so_far"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_agent_id), 2).alias("csat_vs_csat_agent_id_avg"),
        F.round(F.avg("requester_id").over(w_all_by_opened_at_moving_2), 2).alias("requester_id_avg_3"),
        F.last("closed_at", ignorenulls=True).over(w_agent_id_csat_by_csat_runnin).alias("last_known_closed_at"),
        F.nth_value("closed_at", 2).over(w_closed_at_to_date_by_csat_fu).alias("second_closed_at"),
        F.size(F.collect_set("priority").over(w_csat_tier)).alias("distinct_priority_in_csat_tier"),
        F.nth_value("closed_at", 2).over(w_all_by_opened_at_full).alias("second_closed_at_2"),
        F.last("opened_at").over(w_closed_at_to_date_by_csat_fu).alias("last_opened_at"),
        F.size(F.collect_set("csat_tier").over(w_agent_id)).alias("distinct_csat_tier_in_agent_id"),
        F.last("opened_at").over(w_agent_id_csat_by_csat_full).alias("last_opened_at_2"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "opened_at",
        "closed_at",
        "priority",
        "csat_tier",
        "csat_change",
        "requester_id_vs_csat_agent_id_avg",
        "opened_at_rank",
        "min_requester_id_so_far",
        "requester_id_avg_3",
        "second_closed_at",
        "distinct_priority_in_csat_tier",
        "last_opened_at",
        "last_opened_at_2",
        F.round(F.avg("opened_at_rank").over(w_last_known_closed_at_to_date), 2).alias("last_known_closed_at_to_date_last_opened_at_mont"),
        F.max("opened_at_rank").over(w_all_by_opened_at_running).alias("max_opened_at_rank_so_far"),
        F.round((F.unix_timestamp("last_known_closed_at") - F.unix_timestamp(F.lag("last_known_closed_at").over(w_all_by_opened_at))) / 60, 1).alias("minutes_since_prev_last_known_closed_at"),
        F.sum("opened_at_rank").over(w_all_by_opened_at_prior_6).alias("opened_at_rank_sum_6_prior"),
    )
)

rollup_base = (
    with_windows
    .groupBy(F.date_format("closed_at", "yyyy-MM").alias("closed_at_ym"))
    .agg(
        F.sum("distinct_priority_in_csat_tier").alias("total_distinct_priority_in_csat_tier"),
        F.min("closed_at").alias("first_closed_at"),
        F.round(F.sum("last_known_closed_at_to_date_last_opened_at_mont") / F.nullif(F.sum("max_opened_at_rank_so_far"), F.lit(0)), 3).alias("last_known_closed_at_to_date_last_opened_at_mont_2"),
    )
)

with_windows_2 = (
    rollup_base
    .select(
        "*",
        F.nth_value("first_closed_at", 2).over(w_first_closed_at_year_by_last).alias("second_first_closed_at"),
    )
)

result = (
    with_windows_2
    .filter(F.col("closed_at_ym").isNotNull())
    .select(
        "closed_at_ym",
        "total_distinct_priority_in_csat_tier",
        "last_known_closed_at_to_date_last_opened_at_mont_2",
        "second_first_closed_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("second_first_closed_at")).alias("days_since_second_first_closed_at"),
        F.round((F.unix_timestamp(F.col("first_closed_at")) - F.unix_timestamp(F.col("second_first_closed_at"))) / 3600, 1).alias("hours_second_first_closed_at_to_first_closed_at"),
    )
    .orderBy(F.col("hours_second_first_closed_at_to_first_closed_at").desc())
)
