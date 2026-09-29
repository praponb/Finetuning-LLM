from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_team_by_reque = Window.partitionBy("priority", "agent_team").orderBy(F.col("requester_id").desc(), "id")
w_agent_team_by_csat_running = Window.partitionBy("agent_team").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id")
w_channel_agent_team_by_csat_r = Window.partitionBy("channel", "agent_team").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_closed_at_ties = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc())
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_opened_at_month_agent_team = Window.partitionBy(F.month("opened_at"), "agent_team")
w_requester_id_tercile_by_clos = Window.partitionBy("requester_id_tercile").orderBy("closed_at_cume_dist_2", "id")
w_priority_by_running_count_ti = Window.partitionBy("priority").orderBy(F.col("running_count").desc())
w_all_by_csat_ties = Window.orderBy("csat")
w_priority = Window.partitionBy("priority")
w_priority_by_running_count = Window.partitionBy("priority").orderBy(F.col("running_count").desc(), "id")
w_agent_team_n_agent_tier_in_t = Window.partitionBy("agent_team", "n_agent_tier_in_t1_t2").orderBy(F.col("last_closed_at").desc_nulls_first(), "agent_team", "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_n_agent_tier_in_t_2 = Window.partitionBy("agent_team", "n_agent_tier_in_t1_t2").orderBy(F.col("last_closed_at").desc_nulls_first())
w_last_closed_at_to_date = Window.partitionBy(F.to_date("last_closed_at"))

filtered = (
    df_tickets
    .filter(F.col("channel") == "web")
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
    )
)

joined_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    joined_agent
    .select(
        "*",
        F.rank().over(w_priority_agent_team_by_reque).alias("requester_id_rank"),
        F.count(F.lit(1)).over(w_agent_team_by_csat_running).alias("running_count"),
        F.round(F.cume_dist().over(w_agent_tier_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.sum("requester_id").over(w_channel_agent_team_by_csat_r).alias("running_requester_id"),
        F.round(F.cume_dist().over(w_agent_tier_by_closed_at_ties), 3).alias("closed_at_cume_dist_2"),
        F.size(F.collect_set("agent_team").over(w_agent_id_csat)).alias("distinct_agent_team_in_agent_id_csat"),
        F.count(F.lit(1)).over(w_opened_at_month_agent_team).alias("rows_in_opened_at_month_agent_team"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
        F.ntile(3).over(w_priority_agent_team_by_reque).alias("requester_id_tercile"),
        F.rank().over(w_agent_tier_by_closed_at_ties).alias("closed_at_rank"),
        F.size(F.collect_set("agent_tier").over(w_agent_id_csat)).alias("distinct_agent_tier_in_agent_id_csat"),
        F.lag("requester_id", 3, 0).over(w_agent_tier_by_closed_at).alias("prev3_requester_id"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_agent_tier_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.ntile(3).over(w_requester_id_tercile_by_clos).alias("closed_at_cume_dist_2_tercile"),
        F.dense_rank().over(w_priority_by_running_count_ti).alias("running_count_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_csat_ties), 3).alias("csat_cume_dist"),
        F.max("closed_at_cume_dist_2").over(w_priority).alias("priority_max_closed_at_cume_dist_2"),
        F.rank().over(w_requester_id_tercile_by_clos).alias("closed_at_cume_dist_2_rank"),
        F.lead("prev3_requester_id", 3).over(w_priority_by_running_count).alias("next3_prev3_requester_id"),
        F.min("closed_at_cume_dist").over(w_priority).alias("priority_min_closed_at_cume_dist"),
    )
)

summary = (
    metrics
    .filter(F.col("minutes_since_prev_closed_at") <= 3750.0)
    .groupBy(
        "agent_team",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.sum(F.when(F.col("agent_tier").isin("T1", "T2"), 1).otherwise(0)).alias("n_agent_tier_in_t1_t2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("closed_at").alias("last_closed_at"),
        F.sum("requester_id").alias("total_requester_id"),
        F.avg("requester_id_rank").alias("avg_requester_id_rank_check"),
    )
    .filter(F.col("avg_requester_id_rank_check") > 4)
    .drop("avg_requester_id_rank_check")
)

windowed = (
    summary
    .select(
        "*",
        F.first("total_requester_id", ignorenulls=True).over(w_agent_team_n_agent_tier_in_t).alias("first_known_total_requester_id"),
        F.round(F.percent_rank().over(w_agent_team_n_agent_tier_in_t_2), 3).alias("last_closed_at_percent_rank"),
        F.round(F.col("n_agent_tier_in_t1_t2") / F.nullif(F.sum("n_agent_tier_in_t1_t2").over(w_last_closed_at_to_date), F.lit(0)), 4).alias("n_agent_tier_in_t1_t2_share_of_last_closed_at_to"),
        F.round(F.cume_dist().over(w_agent_team_n_agent_tier_in_t_2), 3).alias("last_closed_at_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "agent_team",
        "opened_at_month",
        "n_agent_tier_in_t1_t2",
        "distinct_ids",
        "last_closed_at",
        "total_requester_id",
        "last_closed_at_percent_rank",
        "n_agent_tier_in_t1_t2_share_of_last_closed_at_to",
        "last_closed_at_cume_dist",
        F.date_format(F.col("opened_at_month"), "yyyy-MM").alias("opened_at_month_label"),
        F.to_date(F.col("opened_at_month")).alias("opened_at_month_date"),
    )
    .orderBy(F.col("last_closed_at_percent_rank").asc_nulls_last())
)
