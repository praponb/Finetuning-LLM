from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())
w_all_by_csat_prior_3 = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)
w_channel_by_csat_moving_1 = Window.partitionBy("channel").orderBy("csat", "id").rowsBetween(-1, Window.currentRow)
w_channel_opened_at_month = Window.partitionBy("channel", F.date_trunc("month", "opened_at"))
w_all_by_csat_full = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_channel_by_csat_full = Window.partitionBy("channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id")
w_all_by_csat_prior_1 = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(-1, -1)
w_all_by_csat_running = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_csat_distinct_age = Window.partitionBy("last_known_csat", "distinct_agent_id_in_channel_opened_at_month")
w_distinct_agent_id_in_channel = Window.partitionBy("distinct_agent_id_in_channel_opened_at_month").orderBy(F.col("csat_max_3_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_agent_id_in_channel_2 = Window.partitionBy("distinct_agent_id_in_channel_opened_at_month").orderBy(F.col("csat_max_3_prior").desc(), "id")
w_all_by_csat_percent_rank_2_m = Window.orderBy(F.col("csat_percent_rank_2").desc(), "id").rowsBetween(-3, Window.currentRow)
w_distinct_agent_id_in_channel_3 = Window.partitionBy("distinct_agent_id_in_channel_opened_at_month").orderBy(F.col("csat_max_3_prior").desc(), "id").rowsBetween(-1, Window.currentRow)
w_agent_id_last_known_csat_by = Window.partitionBy("agent_id", "last_known_csat").orderBy("csat_sum_2", "id")

valid = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-02-22").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_parent_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T1", "T2")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_slug",
        "su.agent_name",
    )
)

with_trends = (
    with_parent_agent
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.max("csat").over(w_all_by_csat_prior_3).alias("csat_max_3_prior"),
        F.sum("csat").over(w_channel_by_csat_moving_1).alias("csat_sum_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel_opened_at_month), 2).alias("csat_vs_channel_opened_at_month_avg"),
        F.min("requester_id").over(w_channel_opened_at_month).alias("channel_opened_at_month_min_requester_id"),
        F.last("requester_id").over(w_all_by_csat_full).alias("last_requester_id"),
        F.round(F.percent_rank().over(w_all_by_csat), 3).alias("csat_percent_rank_2"),
        F.count(F.lit(1)).over(w_channel_opened_at_month).alias("rows_in_channel_opened_at_month"),
        F.nth_value("opened_at", 2).over(w_channel_by_csat_full).alias("second_opened_at"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.row_number().over(w_csat_by_csat).alias("seq_no"),
        F.round(F.avg("csat").over(w_all_by_csat_prior_1), 2).alias("csat_avg_1_prior"),
        F.last("csat", ignorenulls=True).over(w_all_by_csat_running).alias("last_known_csat"),
        F.sum("requester_id").over(w_channel_opened_at_month).alias("channel_opened_at_month_sum_requester_id"),
        F.size(F.collect_set("agent_id").over(w_channel_opened_at_month)).alias("distinct_agent_id_in_channel_opened_at_month"),
    )
)

leaders = (
    with_trends
    .filter(F.col("csat_rank") <= 2)
    .drop("csat_rank")
)

windowed = (
    leaders
    .select(
        "*",
        F.count(F.lit(1)).over(w_last_known_csat_distinct_age).alias("rows_in_last_known_csat_distinct_agent_id_in_cha"),
        F.last("csat_percent_rank").over(w_distinct_agent_id_in_channel).alias("last_csat_percent_rank"),
        F.lead("opened_at").over(w_distinct_agent_id_in_channel_2).alias("next_opened_at"),
        F.sum("seq_no").over(w_all_by_csat_percent_rank_2_m).alias("seq_no_sum_4"),
        F.max("requester_id").over(w_distinct_agent_id_in_channel_3).alias("requester_id_max_2"),
        F.row_number().over(w_agent_id_last_known_csat_by).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "id",
        "agent_id",
        "channel",
        "tags",
        "channel_slug",
        "agent_name",
        "csat_percent_rank",
        "csat_max_3_prior",
        "last_requester_id",
        "csat_percent_rank_2",
        "rows_in_channel_opened_at_month",
        "second_opened_at",
        "seq_no",
        "csat_avg_1_prior",
        "last_known_csat",
        "distinct_agent_id_in_channel_opened_at_month",
        "rows_in_last_known_csat_distinct_agent_id_in_cha",
        "last_csat_percent_rank",
        "next_opened_at",
        "seq_no_sum_4",
        "requester_id_max_2",
        F.size(F.col("tags")).alias("tags_count"),
    )
    .orderBy("seq_no", F.col("last_csat_percent_rank").desc())
)
