from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_tier_by_close = Window.partitionBy("priority", "agent_tier").orderBy("closed_at", "id")
w_agent_tier_agent_id_by_csat = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("csat").desc())
w_agent_id = Window.partitionBy("agent_id")
w_channel_csat = Window.partitionBy("channel", "csat")
w_channel_agent_id_by_agent_hi = Window.partitionBy("channel", "agent_id").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id")
w_channel_agent_id_by_agent_hi_2 = Window.partitionBy("channel", "agent_id").orderBy(F.col("agent_hired_on").asc_nulls_last())
w_agent_id_agent_tier_by_rows = Window.partitionBy("agent_id", "agent_tier").orderBy("rows_in_channel_csat")
w_agent_tier_closed_at_month_b = Window.partitionBy("agent_tier", F.date_trunc("month", "closed_at")).orderBy(F.col("requester_id_share_of_channel_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_dense_rank_opened_at_ye = Window.partitionBy("csat_dense_rank", F.year("opened_at")).orderBy(F.col("requester_id_share_of_channel_csat").desc(), "id")
w_priority_csat_by_requester_i = Window.partitionBy("priority", "csat").orderBy(F.col("requester_id_share_of_channel_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")
w_agent_id_agent_tier_by_rows_2 = Window.partitionBy("agent_id", "agent_tier").orderBy("rows_in_channel_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tickets
    .filter(F.col("priority") != "P1")
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
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

joined = (
    recent.alias("s")
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
        "s.tags",
        "s.channel_clean",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.rank().over(w_priority_agent_tier_by_close).alias("closed_at_rank"),
        F.dense_rank().over(w_agent_tier_agent_id_by_csat).alias("csat_dense_rank"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
        F.count(F.lit(1)).over(w_channel_csat).alias("rows_in_channel_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_csat), F.lit(0)), 4).alias("requester_id_share_of_channel_csat"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_channel_agent_id_by_agent_hi))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.percent_rank().over(w_channel_agent_id_by_agent_hi_2), 3).alias("agent_hired_on_percent_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_id_agent_tier_by_rows), 3).alias("rows_in_channel_csat_percent_rank"),
        F.sum("agent_hired_on_percent_rank").over(w_agent_tier_closed_at_month_b).alias("running_agent_hired_on_percent_rank"),
        F.lag("csat").over(w_csat_dense_rank_opened_at_ye).alias("prev_csat"),
        F.nth_value("csat", 2).over(w_priority_csat_by_requester_i).alias("second_csat"),
        F.max("csat_dense_rank").over(w_priority).alias("priority_max_csat_dense_rank"),
        F.round(F.col("agent_id_max_requester_id") - F.avg("agent_id_max_requester_id").over(w_priority), 2).alias("agent_id_max_requester_id_vs_priority_avg"),
        F.first("requester_id").over(w_agent_id_agent_tier_by_rows_2).alias("first_requester_id"),
        F.min("closed_at_rank").over(w_priority).alias("priority_min_closed_at_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("requester_id") <= 21)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "channel_clean",
        "agent_name",
        "closed_at_rank",
        "csat_dense_rank",
        "agent_id_max_requester_id",
        "minutes_since_prev_opened_at",
        "agent_hired_on_percent_rank",
        "rows_in_channel_csat_percent_rank",
        "running_agent_hired_on_percent_rank",
        "prev_csat",
        "second_csat",
        "priority_max_csat_dense_rank",
        "agent_id_max_requester_id_vs_priority_avg",
        "first_requester_id",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)
