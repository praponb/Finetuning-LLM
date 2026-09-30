from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) | (F.col("priority") == "P1"))
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
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

channel_stats = (
    with_details
    .groupBy("channel")
    .agg(F.avg("csat").alias("avg_csat"))
)

vs_group = (
    with_details.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "reopened"), "left_anti")
    .filter(F.col("csat") > 1)
)

combined_details_agent = (
    qualified.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isin("Tech", "VIP", "Onboarding")), "left")
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
        "s.agent_hired_on",
        "s.avg_csat",
        "s.csat_minus_avg",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    combined_details_agent
    .groupingSets(
        [["agent_team", "channel"], ["agent_team"], ["channel"]],
        "agent_team", "channel",
    )
    .agg(
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat_minus_avg"), F.lit(0)), 3).alias("requester_id_to_csat_minus_avg_ratio"),
    )
    .orderBy(F.col("agent_team").asc_nulls_last(), F.col("channel").asc_nulls_last())
)
