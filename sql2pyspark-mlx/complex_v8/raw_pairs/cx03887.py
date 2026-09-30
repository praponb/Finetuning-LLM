from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month = Window.partitionBy(F.date_trunc("month", "opened_at"))
w_agent_id = Window.partitionBy("agent_id")
w_priority = Window.partitionBy("priority")
w_agent_team_closed_at_month_b = Window.partitionBy("agent_team", F.month("closed_at")).orderBy("csat", "id")
w_csat_by_closed_at = Window.partitionBy("csat").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_csat_by_requester_id = Window.partitionBy("csat").orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_all_by_agent_hired_on_runnin = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_closed_at_month_b_2 = Window.partitionBy("agent_team", F.month("closed_at")).orderBy("csat", "id").rowsBetween(-2, -1)
w_csat_quartile_distinct_agent = Window.partitionBy("csat_quartile", "distinct_agent_team_in_agent_id").orderBy("opened_at_month_min_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month_distinct_age = Window.partitionBy(F.month("closed_at"), "distinct_agent_team_in_agent_id")
w_closed_at_quintile_by_distin = Window.partitionBy("closed_at_quintile").orderBy(F.col("distinct_agent_team_in_priority").asc_nulls_last(), "id")
w_csat_quartile_distinct_agent_2 = Window.partitionBy("csat_quartile", "distinct_agent_team_in_agent_id").orderBy("opened_at_month_min_requester_id", "id")

prepared_tickets = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("priority") == "P2"))
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
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

combined_details = (
    prepared_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.priority_slug",
        "s.channel_clean",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_opened_at_month), F.lit(0)), 4).alias("csat_share_of_opened_at_month"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.max("csat").over(w_priority).alias("priority_max_csat"),
        F.ntile(4).over(w_agent_team_closed_at_month_b).alias("csat_quartile"),
        F.sum("requester_id").over(w_agent_id).alias("agent_id_sum_requester_id"),
        F.size(F.collect_set("agent_team").over(w_agent_id)).alias("distinct_agent_team_in_agent_id"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_csat_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.cume_dist().over(w_csat_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.ntile(5).over(w_csat_by_closed_at).alias("closed_at_quintile"),
        (F.col("csat") - F.lag("csat").over(w_csat_by_requester_id)).alias("csat_change"),
        F.count(F.lit(1)).over(w_all_by_agent_hired_on_runnin).alias("running_count"),
        F.min("requester_id").over(w_opened_at_month).alias("opened_at_month_min_requester_id"),
        F.count(F.lit(1)).over(w_priority).alias("rows_in_priority"),
        F.round(F.avg("requester_id").over(w_agent_team_closed_at_month_b_2), 2).alias("requester_id_avg_2_prior"),
        F.size(F.collect_set("agent_team").over(w_priority)).alias("distinct_agent_team_in_priority"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.when(F.col("csat_quartile") >= 7, "HIGH").when(F.col("csat_quartile") >= 5, "MEDIUM").otherwise("LOW").alias("csat_quartile_tier"),
        F.round(F.col("requester_id_avg_2_prior") * 1.5, 2).alias("requester_id_avg_2_prior_adj"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "priority_slug",
        "channel_clean",
        "agent_team",
        "priority_max_csat",
        "agent_id_sum_requester_id",
        "distinct_agent_team_in_agent_id",
        "minutes_since_prev_closed_at",
        "closed_at_cume_dist",
        "closed_at_quintile",
        "running_count",
        "opened_at_month_min_requester_id",
        "rows_in_priority",
        "distinct_agent_team_in_priority",
        "csat_quartile_tier",
        F.last("closed_at").over(w_csat_quartile_distinct_agent).alias("last_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_month_distinct_age).alias("rows_in_closed_at_month_distinct_agent_team_in_a"),
        F.ntile(5).over(w_closed_at_quintile_by_distin).alias("distinct_agent_team_in_priority_quintile"),
        F.size(F.collect_set("csat_quartile_tier").over(w_closed_at_month_distinct_age)).alias("distinct_csat_quartile_tier_in_closed_at_month_d"),
        F.round(F.cume_dist().over(w_csat_quartile_distinct_agent_2), 3).alias("opened_at_month_min_requester_id_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "priority_slug",
        "agent_team",
        "agent_id_sum_requester_id",
        "distinct_agent_team_in_agent_id",
        "minutes_since_prev_closed_at",
        "closed_at_cume_dist",
        "closed_at_quintile",
        "running_count",
        "opened_at_month_min_requester_id",
        "rows_in_priority",
        "csat_quartile_tier",
        "last_closed_at",
        "rows_in_closed_at_month_distinct_agent_team_in_a",
        "distinct_agent_team_in_priority_quintile",
        "distinct_csat_quartile_tier_in_closed_at_month_d",
        "opened_at_month_min_requester_id_cume_dist",
    )
    .orderBy("distinct_agent_team_in_agent_id")
)
