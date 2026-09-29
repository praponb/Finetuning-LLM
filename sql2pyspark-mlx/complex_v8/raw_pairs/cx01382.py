from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").asc_nulls_last(), "id")
w_csat_by_csat_full = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority = Window.partitionBy("priority")
w_priority_by_csat_prior_1 = Window.partitionBy("priority").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-1, -1)

recent_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) | (F.col("channel").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_details_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_filled",
        "s.requester_id_plus_csat",
        "s.channel_priority_label",
        F.col("su.team").alias("agent_team"),
    )
)

tagged = (
    with_details_agent
    .select(
        "*",
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.abs(F.col("requester_id_filled") - 12).alias("requester_id_filled_distance"),
    )
)

with_windows = (
    tagged
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "requester_id_plus_csat",
        F.ntile(4).over(w_priority_by_csat).alias("csat_quartile"),
        F.last("csat").over(w_csat_by_csat_full).alias("last_csat"),
        F.min("csat").over(w_priority).alias("priority_min_csat"),
        F.round(F.avg("closed_at_year").over(w_priority_by_csat_prior_1), 2).alias("closed_at_year_avg_1_prior"),
        F.lead("closed_at_year", 1, 0).over(w_priority_by_csat).alias("next_closed_at_year"),
        F.round(F.col("requester_id_filled") - F.avg("requester_id_filled").over(w_priority), 2).alias("requester_id_filled_vs_priority_avg"),
    )
)

with_parent_agent = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.requester_id_plus_csat",
        "s.last_csat",
        "s.priority_min_csat",
        "s.closed_at_year_avg_1_prior",
        "s.next_closed_at_year",
        "s.requester_id_filled_vs_priority_avg",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    with_parent_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "priority_min_csat",
        "closed_at_year_avg_1_prior",
        "next_closed_at_year",
        "agent_tier",
        "agent_name",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
    .orderBy(
        F.col("closed_at_year_avg_1_prior").asc_nulls_last(),
        F.col("next_closed_at_year").desc(),
    )
)
