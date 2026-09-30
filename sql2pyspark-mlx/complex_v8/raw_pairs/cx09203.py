from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 3) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
        F.size(F.col("tags")).alias("tags_count"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

with_parent_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("J%")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_clean",
        "s.tags_count",
        "s.csat_plus_requester_id",
        "su.agent_name",
    )
)

without_matches = (
    with_parent_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "created"), "left_anti")
)

with_details_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier").isNotNull())
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_clean",
        "s.tags_count",
        "s.csat_plus_requester_id",
        "s.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details_agent
    .groupBy("agent_tier")
    .pivot("channel", ["email", "chat", "phone", "web"])
    .agg(F.max("csat_plus_requester_id"))
)
