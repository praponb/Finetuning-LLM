from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_channel_closed_at_month_by_c = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("closed_at", "id")
w_channel_priority_by_opened_a = Window.partitionBy("channel", "priority").orderBy(F.col("opened_at").desc(), "id")
w_channel_closed_at_month_by_c_2 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("closed_at", "id").rowsBetween(-1, -1)
w_channel_priority_by_opened_a_2 = Window.partitionBy("channel", "priority").orderBy(F.col("opened_at").desc())
w_channel_priority_by_opened_a_3 = Window.partitionBy("channel", "priority").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_closed_at_date_r = Window.partitionBy("agent_id").orderBy("closed_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_closed_at_month_by_c_3 = Window.partitionBy("channel", F.date_trunc("month", "closed_at")).orderBy("closed_at")
w_n_channel_chat_by_min_opened = Window.partitionBy("n_channel_chat").orderBy("min_opened_at_cume_dist", "prev_opened_at_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_channel_chat = Window.partitionBy("n_channel_chat")
w_n_channel_chat_by_min_opened_2 = Window.partitionBy("n_channel_chat").orderBy("min_opened_at_cume_dist")
w_n_channel_chat_by_min_opened_3 = Window.partitionBy("n_channel_chat").orderBy("min_opened_at_cume_dist", "prev_opened_at_day").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tickets = (
    df_tickets
    .filter((~F.col("channel").isin("phone", "chat")) & (F.hour(F.col("closed_at")).between(9, 14)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

sequenced = (
    recent_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "closed_at_date",
        "priority_clean",
        F.round(F.col("csat") - F.avg("csat").over(w_priority), 2).alias("csat_vs_priority_avg"),
        F.lag("opened_at").over(w_channel_closed_at_month_by_c).alias("prev_opened_at"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_channel_priority_by_opened_a))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.max("requester_id").over(w_channel_closed_at_month_by_c_2).alias("requester_id_max_1_prior"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority), F.lit(0)), 4).alias("requester_id_share_of_priority"),
        F.round(F.cume_dist().over(w_channel_priority_by_opened_a_2), 3).alias("opened_at_cume_dist"),
        F.last("closed_at").over(w_channel_priority_by_opened_a_3).alias("last_closed_at"),
        F.max("csat").over(w_priority).alias("priority_max_csat"),
        F.count(F.lit(1)).over(w_agent_id_by_closed_at_date_r).alias("running_count"),
        F.ntile(3).over(w_channel_closed_at_month_by_c).alias("closed_at_tercile"),
        F.rank().over(w_channel_closed_at_month_by_c_3).alias("closed_at_rank"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("closed_at_rank") <= 3) & (F.col("last_closed_at") >= F.lit("2025-01-15 08:00:00").cast("timestamp")))
    .drop("closed_at_rank")
)

grouped = (
    top_per_group
    .groupBy(F.to_date("prev_opened_at").alias("prev_opened_at_day"))
    .agg(
        F.round(100 * F.count(F.when(F.col("channel").isNotNull(), 1)) / F.count("*"), 1).alias("pct_channel_is_not_null"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("channel") == "phone", 1).otherwise(0)).alias("n_channel_phone"),
        F.sum(F.when(F.col("channel") == "chat", 1).otherwise(0)).alias("n_channel_chat"),
        F.round(F.sum("csat_vs_priority_avg") / F.nullif(F.sum("priority_max_csat"), F.lit(0)), 3).alias("csat_vs_priority_avg_to_priority_max_csat_ratio"),
        F.min("opened_at_cume_dist").alias("min_opened_at_cume_dist"),
        F.max("csat_vs_priority_avg").alias("max_csat_vs_priority_avg_check"),
    )
    .filter(F.col("max_csat_vs_priority_avg_check") > -3.5)
    .drop("max_csat_vs_priority_avg_check")
)

with_trends = (
    grouped
    .select(
        "prev_opened_at_day",
        "pct_channel_is_not_null",
        "channel_list",
        "n_channel_phone",
        "n_channel_chat",
        "csat_vs_priority_avg_to_priority_max_csat_ratio",
        "min_opened_at_cume_dist",
        F.sum("n_channel_phone").over(w_n_channel_chat_by_min_opened).alias("running_n_channel_phone"),
        F.sum("min_opened_at_cume_dist").over(w_n_channel_chat_by_min_opened).alias("running_min_opened_at_cume_dist"),
        F.round(F.percent_rank().over(w_n_channel_chat_by_min_opened_2), 3).alias("min_opened_at_cume_dist_percent_rank"),
        F.nth_value("min_opened_at_cume_dist", 2).over(w_n_channel_chat_by_min_opened_3).alias("second_min_opened_at_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "prev_opened_at_day",
        "pct_channel_is_not_null",
        "channel_list",
        "n_channel_phone",
        "n_channel_chat",
        "csat_vs_priority_avg_to_priority_max_csat_ratio",
        "running_n_channel_phone",
        "running_min_opened_at_cume_dist",
        "min_opened_at_cume_dist_percent_rank",
        "second_min_opened_at_cume_dist",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_opened_at_day")).alias("days_since_prev_opened_at_day"),
    )
    .orderBy(F.col("prev_opened_at_day").desc())
)
