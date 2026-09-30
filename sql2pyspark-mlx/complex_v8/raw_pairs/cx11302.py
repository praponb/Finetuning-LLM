from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

with_parent_agent = (
    with_activity.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

with_details_agent = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("J%")), "inner")
    .filter(F.col("su.tier").isin("T1", "T3"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.agent_team",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_parent = (
    with_details_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.agent_team",
        "s.agent_tier",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.team").alias("agent_team_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        "t.first_ticket_opened_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    with_stats.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "agent_team",
        "agent_tier_2",
        "agent_team_2",
        "first_ticket_opened_at",
        "event_count",
    )
)
