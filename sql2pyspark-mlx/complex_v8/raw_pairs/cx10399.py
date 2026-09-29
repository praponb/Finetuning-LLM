from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_year_csat_distance = Window.partitionBy(F.year("opened_at"), "csat_distance").orderBy("csat_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_distance_priority = Window.partitionBy("csat_distance", "priority")
w_channel_len_by_channel_len_f = Window.partitionBy("channel_len").orderBy(F.col("channel_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")
w_all_by_closed_at_running = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_by_csat_ties = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy("csat")
w_closed_at_month_by_csat = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy("csat", "id")
w_opened_at_year_csat_change_b = Window.partitionBy(F.year("opened_at"), "csat_change").orderBy("channel_len_vs_priority_avg")
w_csat_dense_rank_agent_tier_b = Window.partitionBy("csat_dense_rank", "agent_tier").orderBy("opened_at", "id")
w_csat_csat_distance_by_csat_d = Window.partitionBy("csat", "csat_distance").orderBy("csat_distance_share_of_csat_distance_priority", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_year_csat_change_b_2 = Window.partitionBy(F.year("opened_at"), "csat_change").orderBy("channel_len_vs_priority_avg", "id")

valid = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(2, 3)) & (F.col("agent_id") >= 8))
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
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.csat_distance",
        "s.channel_len",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.max("csat").over(w_opened_at_year_csat_distance).alias("max_csat_so_far"),
        F.sum("csat_distance").over(w_csat_distance_priority).alias("csat_distance_priority_sum_csat_distance"),
        F.nth_value("csat", 2).over(w_channel_len_by_channel_len_f).alias("second_csat"),
        F.round(F.col("channel_len") - F.avg("channel_len").over(w_priority), 2).alias("channel_len_vs_priority_avg"),
        F.first("csat_distance", ignorenulls=True).over(w_all_by_closed_at_running).alias("first_known_csat_distance"),
        F.dense_rank().over(w_closed_at_month_by_csat_ties).alias("csat_dense_rank"),
        F.size(F.collect_set("agent_team").over(w_csat_distance_priority)).alias("distinct_agent_team_in_csat_distance_priority"),
        F.rank().over(w_closed_at_month_by_csat_ties).alias("csat_rank"),
        (F.col("csat") - F.lag("csat").over(w_closed_at_month_by_csat)).alias("csat_change"),
        F.round(F.col("channel_len") / F.nullif(F.sum("channel_len").over(w_csat_distance_priority), F.lit(0)), 4).alias("channel_len_share_of_csat_distance_priority"),
        F.round(F.col("csat_distance") / F.nullif(F.sum("csat_distance").over(w_csat_distance_priority), F.lit(0)), 4).alias("csat_distance_share_of_csat_distance_priority"),
    )
)

top_rows = windowed.filter(F.col("csat_rank") == 1)

windowed_2 = (
    top_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_opened_at_year_csat_change_b), 3).alias("channel_len_vs_priority_avg_percent_rank"),
        F.lag("closed_at").over(w_csat_dense_rank_agent_tier_b).alias("prev_closed_at"),
        F.first("csat", ignorenulls=True).over(w_csat_csat_distance_by_csat_d).alias("first_known_csat"),
        F.lag("closed_at", 2).over(w_opened_at_year_csat_change_b_2).alias("prev2_closed_at"),
        F.rank().over(w_opened_at_year_csat_change_b_2).alias("channel_len_vs_priority_avg_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "channel_len",
        "agent_tier",
        "csat_distance_priority_sum_csat_distance",
        "second_csat",
        "channel_len_vs_priority_avg",
        "first_known_csat_distance",
        "csat_dense_rank",
        "distinct_agent_team_in_csat_distance_priority",
        "channel_len_share_of_csat_distance_priority",
        "csat_distance_share_of_csat_distance_priority",
        "prev2_closed_at",
        "channel_len_vs_priority_avg_rank",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.least(F.col("second_csat"), F.col("csat_rank")).alias("least_second_csat_csat_rank"),
    )
    .orderBy("id")
    .limit(5)
)
