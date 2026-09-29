from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_greatest_requeste = Window.partitionBy("channel").orderBy("greatest_requester_id_csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team_priority_group = Window.partitionBy("agent_team", "priority_group")
w_channel_by_greatest_requeste_2 = Window.partitionBy("channel").orderBy("greatest_requester_id_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_by_greatest_requeste_3 = Window.partitionBy("channel").orderBy("greatest_requester_id_csat", "id")
w_channel_by_greatest_requeste_4 = Window.partitionBy("channel").orderBy("greatest_requester_id_csat")
w_priority_group_agent_team_by = Window.partitionBy("priority_group", "agent_team").orderBy("greatest_requester_id_csat_tercile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_group_by_agent_team = Window.partitionBy("priority_group").orderBy("agent_team_priority_group_sum_greatest_requester", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_group_by_agent_team_2 = Window.partitionBy("priority_group").orderBy("agent_team_priority_group_sum_greatest_requester", "id")

scoped = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp")) & (F.col("agent_id") <= 8))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P2", "P4"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

with_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.priority_group",
        "s.channel_or_unknown",
        "s.greatest_requester_id_csat",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    with_details_agent
    .select(
        "*",
        F.last("greatest_requester_id_csat").over(w_channel_by_greatest_requeste).alias("last_greatest_requester_id_csat"),
        F.size(F.collect_set("agent_id").over(w_agent_team_priority_group)).alias("distinct_agent_id_in_agent_team_priority_group"),
        F.first("csat", ignorenulls=True).over(w_channel_by_greatest_requeste_2).alias("first_known_csat"),
        F.ntile(3).over(w_channel_by_greatest_requeste_3).alias("greatest_requester_id_csat_tercile"),
        F.round(F.percent_rank().over(w_channel_by_greatest_requeste_4), 3).alias("greatest_requester_id_csat_percent_rank"),
        F.sum("greatest_requester_id_csat").over(w_agent_team_priority_group).alias("agent_team_priority_group_sum_greatest_requester"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.max("first_known_csat").over(w_priority_group_agent_team_by).alias("max_first_known_csat_so_far"),
        F.nth_value("closed_at", 2).over(w_priority_group_by_agent_team).alias("second_closed_at"),
        F.rank().over(w_priority_group_by_agent_team_2).alias("agent_team_priority_group_sum_greatest_requester_2"),
    )
)

result = (
    sequenced
    .rollup("csat", "agent_tier")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.stddev("agent_team_priority_group_sum_greatest_requester_2"), 2).alias("stddev_agent_team_priority_group_sum_greatest_re"),
        F.round(F.avg("greatest_requester_id_csat_tercile"), 2).alias("avg_greatest_requester_id_csat_tercile"),
    )
    .select(
        "csat",
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "gid",
        "channel_list",
        "stddev_agent_team_priority_group_sum_greatest_re",
        "avg_greatest_requester_id_csat_tercile",
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("agent_tier").asc_nulls_last())
)
