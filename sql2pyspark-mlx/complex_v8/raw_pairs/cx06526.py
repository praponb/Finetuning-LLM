from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_agent_tier_by_agent_hired_on = Window.partitionBy("agent_tier").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_to_date_by_request = Window.partitionBy(F.to_date("opened_at")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_to_date_by_request_2 = Window.partitionBy(F.to_date("opened_at")).orderBy("requester_id", "id")
w_channel_agent_tier_by_agent = Window.partitionBy("channel", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier = Window.partitionBy("agent_tier")
w_opened_at_to_date_by_request_3 = Window.partitionBy(F.to_date("opened_at")).orderBy("requester_id")
w_agent_tier_by_requester_id_r = Window.partitionBy("agent_tier").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_agent_hired_on_2 = Window.partitionBy("agent_tier").orderBy(F.col("agent_hired_on").desc())
w_agent_tier_by_agent_hired_on_3 = Window.partitionBy("agent_tier").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_tier_by_agent_hired_on_4 = Window.partitionBy("agent_tier").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_requester_id_prior_2 = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(-2, -1)
w_channel_agent_tier_by_reques = Window.partitionBy("channel", "agent_tier").orderBy(F.col("requester_id_vs_channel_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_closed_at_to_date = Window.partitionBy(F.to_date("second_closed_at"))
w_all_by_requester_id_running = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_tier_by_reques_2 = Window.partitionBy("channel", "agent_tier").orderBy(F.col("requester_id_vs_channel_avg").asc_nulls_last(), "id")
w_all_by_last_last_known_agent = Window.orderBy("last_last_known_agent_hired_on", "first_agent_hired_on_month", "opened_at_month")
w_last_last_known_agent_hired = Window.partitionBy(F.date_trunc("month", "last_last_known_agent_hired_on")).orderBy(F.col("last_last_known_agent_hired_on").asc_nulls_last())
w_last_last_known_agent_hired_2 = Window.partitionBy(F.date_trunc("month", "last_last_known_agent_hired_on")).orderBy(F.col("last_last_known_agent_hired_on").asc_nulls_last(), "first_agent_hired_on_month", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_last_known_agent_2 = Window.orderBy("last_last_known_agent_hired_on", "first_agent_hired_on_month", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
    )
)

with_parent_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_parent_agent
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.sum("requester_id").over(w_agent_tier_by_agent_hired_on).alias("running_requester_id"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.first("agent_hired_on").over(w_opened_at_to_date_by_request).alias("first_agent_hired_on"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_opened_at_to_date_by_request_2)).alias("days_since_prev_agent_hired_on"),
        F.nth_value("closed_at", 2).over(w_channel_agent_tier_by_agent).alias("second_closed_at"),
        F.min("requester_id").over(w_agent_tier).alias("agent_tier_min_requester_id"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_to_date_by_request_2))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.size(F.collect_set("priority").over(w_agent_tier)).alias("distinct_priority_in_agent_tier"),
        F.dense_rank().over(w_opened_at_to_date_by_request_3).alias("requester_id_dense_rank"),
        F.last("agent_hired_on", ignorenulls=True).over(w_agent_tier_by_requester_id_r).alias("last_known_agent_hired_on"),
        F.round(F.cume_dist().over(w_agent_tier_by_agent_hired_on_2), 3).alias("agent_hired_on_cume_dist"),
        F.lead("agent_hired_on").over(w_agent_tier_by_agent_hired_on_3).alias("next_agent_hired_on"),
        F.max("requester_id").over(w_agent_tier_by_agent_hired_on_4).alias("requester_id_max_7"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("requester_id_dense_rank") <= 3)
    .drop("requester_id_dense_rank")
)

windowed = (
    top_per_group
    .select(
        "*",
        F.max("requester_id_vs_channel_avg").over(w_all_by_requester_id_prior_2).alias("requester_id_vs_channel_avg_max_2_prior"),
        F.first("rows_in_channel", ignorenulls=True).over(w_channel_agent_tier_by_reques).alias("first_known_rows_in_channel"),
        F.max("agent_tier_min_requester_id").over(w_second_closed_at_to_date).alias("second_closed_at_to_date_max_agent_tier_min_requ"),
        F.first("agent_hired_on", ignorenulls=True).over(w_all_by_requester_id_running).alias("first_known_agent_hired_on"),
        F.count(F.lit(1)).over(w_channel_agent_tier_by_reques).alias("running_count"),
        F.lead("requester_id_max_7", 3).over(w_channel_agent_tier_by_reques_2).alias("next3_requester_id_max_7"),
        F.size(F.collect_set("priority").over(w_second_closed_at_to_date)).alias("distinct_priority_in_second_closed_at_to_date"),
        F.sum("requester_id").over(w_second_closed_at_to_date).alias("second_closed_at_to_date_sum_requester_id"),
    )
)

stats = (
    windowed
    .groupBy(
        F.date_trunc("month", "first_agent_hired_on").alias("first_agent_hired_on_month"),
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.max("last_known_agent_hired_on").alias("last_last_known_agent_hired_on"),
        F.sum("agent_tier_min_requester_id").alias("sum_agent_tier_min_requester_id_check"),
    )
    .filter(F.col("sum_agent_tier_min_requester_id_check") > 63)
    .drop("sum_agent_tier_min_requester_id_check")
)

ranked = (
    stats
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_last_last_known_agent), 3).alias("last_last_known_agent_hired_on_cume_dist"),
        F.round(F.percent_rank().over(w_last_last_known_agent_hired), 3).alias("last_last_known_agent_hired_on_percent_rank"),
        F.last("first_agent_hired_on_month").over(w_last_last_known_agent_hired_2).alias("last_first_agent_hired_on_month"),
        F.first("opened_at_month", ignorenulls=True).over(w_all_by_last_last_known_agent_2).alias("first_known_opened_at_month"),
    )
)

result = (
    ranked
    .filter(F.col("last_last_known_agent_hired_on_cume_dist").between(0.1, 0.8))
    .select(
        "first_agent_hired_on_month",
        "channel_list",
        "last_last_known_agent_hired_on",
        "last_last_known_agent_hired_on_cume_dist",
        "last_first_agent_hired_on_month",
        "first_known_opened_at_month",
        F.date_format(F.col("opened_at_month"), "yyyy-MM-dd HH:00").alias("opened_at_month_label"),
    )
    .orderBy(F.col("last_last_known_agent_hired_on").desc_nulls_first())
)
