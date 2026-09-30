from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_closed_at_full = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_full = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_or_unknown = Window.partitionBy("agent_id", "priority_or_unknown").orderBy("csat", "id")
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_csat_channel_by_requester_id = Window.partitionBy("csat", "channel").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_tier_agent_id = Window.partitionBy("agent_tier", "agent_id")
w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")

base_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

enriched = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()), "left")
    .select("s.*", "su.agent_name", F.col("su.tier").alias("agent_tier"))
)

with_trends = (
    enriched
    .select(
        "*",
        F.nth_value("requester_id", 2).over(w_agent_tier_by_closed_at_full).alias("second_requester_id"),
        F.nth_value("closed_at", 2).over(w_all_by_requester_id_full).alias("second_closed_at"),
        F.round(F.percent_rank().over(w_agent_id_priority_or_unknown), 3).alias("csat_percent_rank"),
        F.ntile(3).over(w_all_by_requester_id).alias("requester_id_tercile"),
        F.rank().over(w_all_by_requester_id_ties).alias("requester_id_rank"),
        F.nth_value("requester_id", 2).over(w_csat_channel_by_requester_id).alias("second_requester_id_2"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier_agent_id), 2).alias("csat_vs_agent_tier_agent_id_avg"),
        F.round(F.cume_dist().over(w_agent_tier_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.lag("closed_at").over(w_agent_id_priority_or_unknown).alias("prev_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_id_csat), F.lit(0)), 4).alias("csat_share_of_agent_id_csat"),
    )
)

result = (
    with_trends
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_or_unknown",
        "agent_name",
        "agent_tier",
        "second_requester_id",
        "second_closed_at",
        "csat_percent_rank",
        "requester_id_tercile",
        "csat_vs_agent_tier_agent_id_avg",
        "closed_at_cume_dist",
        "prev_closed_at",
        "csat_share_of_agent_id_csat",
    )
    .orderBy("second_requester_id", "id")
    .limit(20)
)
