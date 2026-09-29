from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") > 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

without_matches = (
    recent_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("t.actor") == "customer"), "left_semi")
)

with_parent_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.year(F.col("su.hired_on")) == 2025), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.tags_text",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .groupingSets(
        [["agent_team", "agent_tier"], ["agent_team"], ["agent_tier"]],
        "agent_team", "agent_tier",
    )
    .agg(
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
        F.round(100 * F.count(F.when(F.col("channel") == "web", 1)) / F.count("*"), 1).alias("pct_channel_web"),
    )
    .select(
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "distinct_agent_tiers",
        "agent_tier_list",
        "pct_channel_web",
    )
)
