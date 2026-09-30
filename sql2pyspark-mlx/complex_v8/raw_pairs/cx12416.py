from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date_agent_id = Window.partitionBy(F.to_date("opened_at"), "agent_id")
w_priority_by_csat_ties = Window.partitionBy("priority").orderBy("csat")
w_agent_id = Window.partitionBy("agent_id")
w_csat_by_csat_cume_dist_full = Window.partitionBy("csat").orderBy("csat_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat_cume_dist = Window.partitionBy("csat").orderBy("csat_cume_dist", "id")
w_agent_tier_agent_id_by_csat = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("csat_share_of_opened_at_to_date_agent_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_channel = Window.partitionBy("agent_tier", "channel")
w_agent_tier_agent_id_by_csat_2 = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("csat_share_of_opened_at_to_date_agent_id").desc(), "id")

cleaned = (
    df_tickets
    .filter(F.col("priority").isin("P4", "P1", "P3"))
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

windowed = (
    cleaned
    .select(
        "*",
        F.min("csat").over(w_opened_at_to_date_agent_id).alias("opened_at_to_date_agent_id_min_csat"),
        F.round(F.avg("csat").over(w_opened_at_to_date_agent_id), 2).alias("opened_at_to_date_agent_id_avg_csat"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_opened_at_to_date_agent_id), 2).alias("requester_id_vs_opened_at_to_date_agent_id_avg"),
        F.round(F.cume_dist().over(w_priority_by_csat_ties), 3).alias("csat_cume_dist"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_opened_at_to_date_agent_id), F.lit(0)), 4).alias("csat_share_of_opened_at_to_date_agent_id"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.round(F.avg("requester_id").over(w_agent_id), 2).alias("agent_id_avg_requester_id"),
        F.nth_value("requester_id", 2).over(w_csat_by_csat_cume_dist_full).alias("second_requester_id"),
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.round(F.col("csat_cume_dist") / F.nullif(F.sum("csat_cume_dist").over(w_agent_id), F.lit(0)), 4).alias("csat_cume_dist_share_of_agent_id"),
        F.row_number().over(w_csat_by_csat_cume_dist).alias("seq_no"),
    )
)

with_parent_agent = (
    with_trends.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.opened_at_to_date_agent_id_min_csat",
        "s.opened_at_to_date_agent_id_avg_csat",
        "s.requester_id_vs_opened_at_to_date_agent_id_avg",
        "s.csat_share_of_opened_at_to_date_agent_id",
        "s.distinct_channel_in_agent_id",
        "s.agent_id_avg_requester_id",
        "s.second_requester_id",
        "s.agent_id_max_csat",
        "s.csat_cume_dist_share_of_agent_id",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends_2 = (
    with_parent_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "opened_at_to_date_agent_id_min_csat",
        "opened_at_to_date_agent_id_avg_csat",
        "requester_id_vs_opened_at_to_date_agent_id_avg",
        "distinct_channel_in_agent_id",
        "agent_id_avg_requester_id",
        "second_requester_id",
        "agent_id_max_csat",
        "csat_cume_dist_share_of_agent_id",
        F.nth_value("opened_at", 2).over(w_agent_tier_agent_id_by_csat).alias("second_opened_at"),
        F.size(F.collect_set("agent_id").over(w_agent_tier_channel)).alias("distinct_agent_id_in_agent_tier_channel"),
        F.ntile(3).over(w_agent_tier_agent_id_by_csat_2).alias("csat_share_of_opened_at_to_date_agent_id_tercile"),
    )
)

priority_stats = (
    with_trends_2
    .groupBy("priority")
    .agg(F.avg("agent_id_max_csat").alias("avg_agent_id_max_csat"))
)

outliers = (
    with_trends_2.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.agent_id_max_csat") > F.col("g.avg_agent_id_max_csat"))
    .select(
        "s.*",
        "g.avg_agent_id_max_csat",
        F.round(F.col("s.agent_id_max_csat") - F.col("g.avg_agent_id_max_csat"), 2).alias("agent_id_max_csat_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("avg_agent_id_max_csat") < 35.3)
    .select(
        "tags",
        "opened_at_to_date_agent_id_min_csat",
        "distinct_agent_id_in_agent_tier_channel",
    )
    .distinct()
)
