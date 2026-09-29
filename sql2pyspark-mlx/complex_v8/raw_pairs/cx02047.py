from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year_by_opened_at = Window.partitionBy(F.year("closed_at")).orderBy("opened_at", "id")
w_agent_team_channel = Window.partitionBy("agent_team", "channel")

base_tickets = (
    df_tickets
    .filter((F.col("priority") == "P4") & (F.col("closed_at") >= F.lit("2025-01-26 18:00:00").cast("timestamp")))
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
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

channel_stats = (
    base_tickets
    .groupBy("channel")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

vs_group = (
    base_tickets.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T3", "T1")), "inner")
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
        "s.avg_requester_id",
        "s.requester_id_minus_avg",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_windows = (
    with_details
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
        "requester_id_minus_avg",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        F.lead("avg_requester_id").over(w_closed_at_year_by_opened_at).alias("next_avg_requester_id"),
        F.row_number().over(w_closed_at_year_by_opened_at).alias("rn"),
        F.round(F.col("requester_id_minus_avg") / F.nullif(F.sum("requester_id_minus_avg").over(w_agent_team_channel), F.lit(0)), 4).alias("requester_id_minus_avg_share_of_agent_team_chann"),
        F.max("avg_requester_id").over(w_agent_team_channel).alias("agent_team_channel_max_avg_requester_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "requester_id_minus_avg",
        "agent_hired_on",
        "agent_tier",
        "next_avg_requester_id",
        "requester_id_minus_avg_share_of_agent_team_chann",
        "agent_team_channel_max_avg_requester_id",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.month(F.col("closed_at")).alias("closed_at_month"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last())
)
