from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier = Window.partitionBy("agent_tier")
w_closed_at_month_csat_by_agen = Window.partitionBy(F.date_trunc("month", "closed_at"), "csat").orderBy("agent_hired_on", "id")
w_priority = Window.partitionBy("priority")
w_all_by_agent_hired_on_prior = Window.orderBy("agent_hired_on", "id").rowsBetween(-6, -1)
w_closed_at_month_csat_by_agen_2 = Window.partitionBy(F.date_trunc("month", "closed_at"), "csat").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_agent_tier_by_requester_id_p = Window.partitionBy("agent_tier").orderBy(F.col("requester_id_plus_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_csat_by_closed_at_fu = Window.partitionBy("channel", "csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_distinct_agent_ti = Window.partitionBy("agent_tier", "distinct_agent_tier_in_priority")
w_distinct_agent_id_in_agent_t = Window.partitionBy("distinct_agent_id_in_agent_tier").orderBy("requester_id_plus_csat_max_6_prior", "id")
w_distinct_agent_id_in_agent_t_2 = Window.partitionBy("distinct_agent_id_in_agent_tier").orderBy("requester_id_plus_csat_max_6_prior")
w_all_by_days_since_prev_agent = Window.orderBy(F.col("days_since_prev_agent_hired_on").desc(), "id")
w_n_channel_web_by_cnt_full = Window.partitionBy("n_channel_web").orderBy("cnt", "csat", "distinct_agent_id_in_agent_tier_distinct_agent_t").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_channel_web_by_cnt = Window.partitionBy("n_channel_web").orderBy("cnt", "csat", "distinct_agent_id_in_agent_tier_distinct_agent_t")
w_n_channel_web_by_cnt_prior_2 = Window.partitionBy("n_channel_web").orderBy("cnt", "csat", "distinct_agent_id_in_agent_tier_distinct_agent_t").rowsBetween(-2, -1)

valid_tickets = (
    df_tickets
    .filter(F.col("agent_id") <= 7)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

combined_details_agent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    combined_details_agent
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_tier), F.lit(0)), 4).alias("csat_share_of_agent_tier"),
        F.size(F.collect_set("agent_id").over(w_agent_tier)).alias("distinct_agent_id_in_agent_tier"),
        F.row_number().over(w_closed_at_month_csat_by_agen).alias("seq_no"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.size(F.collect_set("agent_tier").over(w_priority)).alias("distinct_agent_tier_in_priority"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier), 2).alias("csat_vs_agent_tier_avg"),
        F.size(F.collect_set("channel").over(w_priority)).alias("distinct_channel_in_priority"),
        F.max("requester_id_plus_csat").over(w_all_by_agent_hired_on_prior).alias("requester_id_plus_csat_max_6_prior"),
        F.nth_value("agent_hired_on", 2).over(w_closed_at_month_csat_by_agen_2).alias("second_agent_hired_on"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_agent_hired_on)).alias("days_since_prev_agent_hired_on"),
        F.last("closed_at", ignorenulls=True).over(w_agent_tier_by_requester_id_p).alias("last_known_closed_at"),
        F.round(F.cume_dist().over(w_closed_at_month_csat_by_agen), 3).alias("agent_hired_on_cume_dist"),
        F.nth_value("closed_at", 2).over(w_channel_csat_by_closed_at_fu).alias("second_closed_at"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.max("requester_id_plus_csat").over(w_agent_tier_distinct_agent_ti).alias("agent_tier_distinct_agent_tier_in_priority_max_r"),
        F.ntile(10).over(w_distinct_agent_id_in_agent_t).alias("requester_id_plus_csat_max_6_prior_decile"),
        F.round(F.percent_rank().over(w_distinct_agent_id_in_agent_t_2), 3).alias("requester_id_plus_csat_max_6_prior_percent_rank"),
        F.size(F.collect_set("agent_id").over(w_agent_tier_distinct_agent_ti)).alias("distinct_agent_id_in_agent_tier_distinct_agent_t"),
        F.dense_rank().over(w_all_by_days_since_prev_agent).alias("days_since_prev_agent_hired_on_dense_rank"),
    )
)

totals = (
    windowed
    .filter(F.col("distinct_agent_id_in_priority") < 9)
    .groupBy("csat", "distinct_agent_id_in_agent_tier_distinct_agent_t")
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("n_channel_web"),
        F.sum(F.when(F.col("agent_tier") != "T3", 1).otherwise(0)).alias("n_agent_tier_t3"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("priority").isNotNull(), F.col("distinct_agent_id_in_agent_tier"))).alias("distinct_agent_id_in_agent_tier_priority_is_not_"),
        F.round(F.sum("csat") / F.nullif(F.sum("agent_tier_distinct_agent_tier_in_priority_max_r"), F.lit(0)), 3).alias("csat_to_agent_tier_distinct_agent_tier_in_priori"),
    )
    .filter(F.col("cnt") >= 2)
)

with_trends = (
    totals
    .select(
        "csat",
        "distinct_agent_id_in_agent_tier_distinct_agent_t",
        "cnt",
        "distinct_agent_id_in_agent_tier_priority_is_not_",
        F.first("csat").over(w_n_channel_web_by_cnt_full).alias("first_csat"),
        F.lag("cnt", 1, 0).over(w_n_channel_web_by_cnt).alias("prev_cnt"),
        F.ntile(3).over(w_n_channel_web_by_cnt).alias("cnt_tercile"),
        F.sum("csat").over(w_n_channel_web_by_cnt_prior_2).alias("csat_sum_2_prior"),
    )
)

result = (
    with_trends
    .select(
        "csat",
        "distinct_agent_id_in_agent_tier_distinct_agent_t",
        "cnt",
        "distinct_agent_id_in_agent_tier_priority_is_not_",
        "csat_sum_2_prior",
        F.coalesce(F.col("first_csat"), F.lit(0)).alias("first_csat_filled"),
        (F.col("cnt") + F.col("csat")).alias("cnt_plus_csat"),
    )
    .orderBy(
        F.col("csat").desc_nulls_first(),
        F.col("csat_sum_2_prior").desc(),
        "distinct_agent_id_in_agent_tier_distinct_agent_t",
    )
    .limit(20)
)
