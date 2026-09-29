from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_csat = Window.partitionBy("channel").orderBy("csat", "id")
w_agent_team_channel_by_opened = Window.partitionBy("agent_team", "channel").orderBy("opened_at", "id")
w_closed_at_year_priority = Window.partitionBy(F.year("closed_at"), "priority")
w_channel_by_csat_running = Window.partitionBy("channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_channel_by_opened_2 = Window.partitionBy("agent_team", "channel").orderBy("opened_at")
w_channel_by_csat_ties = Window.partitionBy("channel").orderBy("csat")
w_agent_tier_csat_by_agent_hir = Window.partitionBy("agent_tier", "csat").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_csat_by_agent_hir_2 = Window.partitionBy("agent_tier", "csat").orderBy("agent_hired_on", "id")
w_opened_at_decile = Window.partitionBy("opened_at_decile")
w_priority_channel_by_last_clo = Window.partitionBy("priority", "channel").orderBy(F.col("last_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(9, 15)) & (F.col("requester_id") > 2))
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

with_details = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.lower(F.col("su.agent_name")).like("%wa%")), "inner")
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
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "agent_team",
        "agent_tier",
        F.dense_rank().over(w_channel_by_csat).alias("csat_dense_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_channel_by_csat))) / 60, 1).alias("minutes_since_prev_closed_at"),
        (F.col("requester_id") - F.lag("requester_id").over(w_agent_team_channel_by_opened)).alias("requester_id_change"),
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_year_priority), 2).alias("csat_vs_closed_at_year_priority_avg"),
        F.first("opened_at", ignorenulls=True).over(w_channel_by_csat_running).alias("first_known_opened_at"),
        F.round(F.percent_rank().over(w_agent_team_channel_by_opened_2), 3).alias("opened_at_percent_rank"),
        F.round(F.percent_rank().over(w_channel_by_csat_ties), 3).alias("csat_percent_rank"),
        F.size(F.collect_set("agent_team").over(w_closed_at_year_priority)).alias("distinct_agent_team_in_closed_at_year_priority"),
        F.min("requester_id").over(w_closed_at_year_priority).alias("closed_at_year_priority_min_requester_id"),
        F.last("closed_at").over(w_agent_tier_csat_by_agent_hir).alias("last_closed_at"),
        F.nth_value("opened_at", 2).over(w_agent_tier_csat_by_agent_hir).alias("second_opened_at"),
        F.count(F.lit(1)).over(w_closed_at_year_priority).alias("rows_in_closed_at_year_priority"),
        F.round(F.percent_rank().over(w_agent_tier_csat_by_agent_hir_2), 3).alias("agent_hired_on_percent_rank"),
        F.ntile(10).over(w_agent_team_channel_by_opened).alias("opened_at_decile"),
        F.round(F.cume_dist().over(w_agent_team_channel_by_opened), 3).alias("opened_at_cume_dist"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "agent_id",
        "priority",
        "tags",
        "agent_team",
        "agent_tier",
        "requester_id_change",
        "first_known_opened_at",
        "opened_at_percent_rank",
        "csat_percent_rank",
        "distinct_agent_team_in_closed_at_year_priority",
        "last_closed_at",
        "agent_hired_on_percent_rank",
        F.round(F.col("opened_at_decile") - F.avg("opened_at_decile").over(w_opened_at_decile), 2).alias("opened_at_decile_vs_opened_at_decile_avg"),
        F.sum("minutes_since_prev_closed_at").over(w_priority_channel_by_last_clo).alias("running_minutes_since_prev_closed_at"),
        F.round(F.col("opened_at_cume_dist") / F.nullif(F.sum("opened_at_cume_dist").over(w_opened_at_decile), F.lit(0)), 4).alias("opened_at_cume_dist_share_of_opened_at_decile"),
        F.round(F.col("requester_id_change") / F.nullif(F.sum("requester_id_change").over(w_opened_at_decile), F.lit(0)), 4).alias("requester_id_change_share_of_opened_at_decile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "priority",
        "tags",
        "agent_team",
        "agent_tier",
        "requester_id_change",
        "first_known_opened_at",
        "opened_at_percent_rank",
        "csat_percent_rank",
        "distinct_agent_team_in_closed_at_year_priority",
        "agent_hired_on_percent_rank",
        "opened_at_decile_vs_opened_at_decile_avg",
        "running_minutes_since_prev_closed_at",
        "requester_id_change_share_of_opened_at_decile",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_closed_at")).alias("days_since_last_closed_at"),
    )
    .orderBy(
        F.col("days_since_last_closed_at").desc(),
        F.col("requester_id_change").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
