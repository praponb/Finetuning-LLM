from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_priority_by = Window.partitionBy(F.date_trunc("month", "opened_at"), "priority").orderBy("requester_id", "id").rowsBetween(-1, Window.currentRow)
w_opened_at_year = Window.partitionBy(F.year("opened_at"))
w_all_by_opened_at_prior_1 = Window.orderBy("opened_at", "id").rowsBetween(-1, -1)
w_all_by_opened_at_running = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_priority_by_2 = Window.partitionBy(F.date_trunc("month", "opened_at"), "priority").orderBy("requester_id", "id")
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_opened_at_month_priority_by_3 = Window.partitionBy(F.date_trunc("month", "opened_at"), "priority").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_percent = Window.orderBy(F.col("requester_id_percent_rank").desc_nulls_first(), "id")
w_priority = Window.partitionBy("priority")
w_priority_by_requester_id_per = Window.partitionBy("priority").orderBy(F.col("requester_id_percent_rank").desc())

recent_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp"))
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
        F.when(F.col("priority").isin("P3", "P2"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_parent_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "su.agent_name",
    )
)

with_trends = (
    with_parent_agent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.sum("csat").over(w_opened_at_month_priority_by).alias("csat_sum_2"),
        F.sum("requester_id").over(w_opened_at_year).alias("opened_at_year_sum_requester_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_year), 2).alias("csat_vs_opened_at_year_avg"),
        F.sum("csat").over(w_all_by_opened_at_prior_1).alias("csat_sum_1_prior"),
        F.count(F.lit(1)).over(w_opened_at_year).alias("rows_in_opened_at_year"),
        F.min("csat").over(w_all_by_opened_at_running).alias("min_csat_so_far"),
        F.size(F.collect_set("agent_id").over(w_opened_at_year)).alias("distinct_agent_id_in_opened_at_year"),
        F.round(F.percent_rank().over(w_opened_at_month_priority_by_2), 3).alias("requester_id_percent_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_opened_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.cume_dist().over(w_opened_at_month_priority_by_2), 3).alias("requester_id_cume_dist"),
        F.first("closed_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("first_known_closed_at"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_opened_at_year), 2).alias("requester_id_vs_opened_at_year_avg"),
        F.first("csat", ignorenulls=True).over(w_opened_at_month_priority_by_3).alias("first_known_csat"),
        F.lag("requester_id", 3).over(w_opened_at_month_priority_by_2).alias("prev3_requester_id"),
    )
)

tagged = (
    with_trends
    .filter(F.col("requester_id_cume_dist") >= 0.2)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "opened_at_year_sum_requester_id",
        "csat_vs_opened_at_year_avg",
        "csat_sum_1_prior",
        "min_csat_so_far",
        "requester_id_percent_rank",
        "minutes_since_prev_closed_at",
        "requester_id_cume_dist",
        "first_known_closed_at",
        "requester_id_vs_opened_at_year_avg",
        "first_known_csat",
        "prev3_requester_id",
        F.concat(F.col("channel"), F.lit("#"), F.col("first_known_csat").cast("string")).alias("channel_ref"),
        F.abs(F.col("csat_sum_2") - 7).alias("csat_sum_2_distance"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.ntile(4).over(w_all_by_requester_id_percent).alias("requester_id_percent_rank_quartile"),
        F.round(F.col("requester_id_cume_dist") - F.avg("requester_id_cume_dist").over(w_priority), 2).alias("requester_id_cume_dist_vs_priority_avg"),
        F.rank().over(w_priority_by_requester_id_per).alias("requester_id_percent_rank_rank"),
    )
)

result = (
    windowed
    .filter(F.col("requester_id_percent_rank_rank") >= 2)
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "opened_at_year_sum_requester_id",
        "csat_vs_opened_at_year_avg",
        "csat_sum_1_prior",
        "min_csat_so_far",
        "minutes_since_prev_closed_at",
        "requester_id_cume_dist",
        "requester_id_vs_opened_at_year_avg",
        "first_known_csat",
        "requester_id_cume_dist_vs_priority_avg",
        "requester_id_percent_rank_rank",
        F.when(F.col("channel").isin("phone", "web"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
    )
)
