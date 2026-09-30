from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("requester_id") >= 11)
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

joined_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "VIP")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

profiled = (
    joined_agent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    profiled
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_channel_label",
        "agent_name",
        "agent_team",
        "event_count",
        "event_count_2",
    )
    .orderBy(F.col("csat").asc_nulls_last(), "channel", "id")
    .limit(10)
)
