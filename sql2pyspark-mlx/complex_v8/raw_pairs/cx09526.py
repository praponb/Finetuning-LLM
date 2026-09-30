from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier = Window.partitionBy("agent_tier")
w_agent_id_priority_len_by_clo = Window.partitionBy("agent_id", "priority_len").orderBy("closed_at_month", "id").rowsBetween(-6, -1)
w_agent_id_priority_len_by_clo_2 = Window.partitionBy("agent_id", "priority_len").orderBy("closed_at_month", "id")

filtered = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.abs(F.col("requester_id") - 12).alias("requester_id_distance"),
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
)

labeled = (
    filtered
    .select(
        "*",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

with_parent = (
    labeled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.priority_len",
        "s.requester_id_distance",
        "s.closed_at_month",
        "s.channel_or_unknown",
        "s.channel_prefix",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "tags",
        "priority_len",
        "closed_at_month",
        "channel_prefix",
        "agent_hired_on",
        F.round(F.col("priority_len") - F.avg("priority_len").over(w_agent_tier), 2).alias("priority_len_vs_agent_tier_avg"),
        F.round(F.avg("requester_id").over(w_agent_id_priority_len_by_clo), 2).alias("requester_id_avg_6_prior"),
        F.round(F.percent_rank().over(w_agent_id_priority_len_by_clo_2), 3).alias("closed_at_month_percent_rank"),
    )
)

with_details_agent = (
    with_windows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.priority_len",
        "s.channel_prefix",
        "s.priority_len_vs_agent_tier_avg",
        "s.requester_id_avg_6_prior",
        "s.closed_at_month_percent_rank",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

labeled_2 = (
    with_details_agent
    .withColumn("agent_hired_on_2_plus_1m", F.add_months(F.col("agent_hired_on_2"), 1))
)

without_matches = (
    labeled_2.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent"), "left_anti")
    .filter(F.upper(F.trim(F.col("agent_name"))).like("L%"))
)

result = (
    without_matches
    .groupBy("agent_id")
    .pivot("agent_team", ["Onboarding", "VIP"])
    .agg(F.avg("closed_at_month_percent_rank"))
    .orderBy("agent_id")
)
