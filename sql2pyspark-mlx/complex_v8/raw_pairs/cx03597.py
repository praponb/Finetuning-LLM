from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.col("requester_id") > 7) & (F.col("opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    scoped_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

joined = (
    with_stats.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T2"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.channel_slug",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_details_agent = (
    joined.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.agent_tier",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_agent
    .rollup("agent_team", "agent_id", "agent_tier")
    .agg(
        F.grouping("agent_team").alias("is_total_agent_team"),
        F.round(100 * F.count(F.when(~F.col("agent_team").isin("Tech", "Billing"), 1)) / F.count("*"), 1).alias("pct_agent_team_not_in_tech_billi"),
    )
    .orderBy(
        F.col("agent_team").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
