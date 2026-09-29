from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_opened_at_month_by = Window.partitionBy("priority", F.month("opened_at")).orderBy("closed_at", "id")
w_agent_team_channel = Window.partitionBy("agent_team", "channel")
w_priority_opened_at_month_by_2 = Window.partitionBy("priority", F.month("opened_at")).orderBy("closed_at", "id").rowsBetween(-6, -1)
w_priority_opened_at_month_by_3 = Window.partitionBy("priority", F.month("opened_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_tier_by_csat = Window.partitionBy("channel", "agent_tier").orderBy("csat", "id")
w_channel = Window.partitionBy("channel")
w_channel_agent_tier_by_csat_r = Window.partitionBy("channel", "agent_tier").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter(F.col("channel") == "email")
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

joined = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.ntile(3).over(w_priority_opened_at_month_by).alias("closed_at_tercile"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_priority_opened_at_month_by))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.count(F.lit(1)).over(w_agent_team_channel).alias("rows_in_agent_team_channel"),
        F.max("csat").over(w_priority_opened_at_month_by_2).alias("csat_max_6_prior"),
        F.max("csat").over(w_priority_opened_at_month_by_3).alias("max_csat_so_far"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "agent_team",
        "closed_at_tercile",
        "minutes_since_prev_opened_at",
        "csat_max_6_prior",
        F.lead("closed_at_tercile").over(w_channel_agent_tier_by_csat).alias("next_closed_at_tercile"),
        F.size(F.collect_set("agent_tier").over(w_channel)).alias("distinct_agent_tier_in_channel"),
        F.first("minutes_since_prev_opened_at", ignorenulls=True).over(w_channel_agent_tier_by_csat_r).alias("first_known_minutes_since_prev_opened_at"),
        F.lead("csat_max_6_prior", 2).over(w_channel_agent_tier_by_csat).alias("next2_csat_max_6_prior"),
        F.round(F.col("max_csat_so_far") - F.avg("max_csat_so_far").over(w_channel), 2).alias("max_csat_so_far_vs_channel_avg"),
    )
)

result = (
    ranked
    .cube("priority", "agent_id", "agent_team")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("priority").isin("P2", "P1", "P4"), 1).otherwise(0)).alias("n_priority_in_p2_p1_p4"),
    )
)
