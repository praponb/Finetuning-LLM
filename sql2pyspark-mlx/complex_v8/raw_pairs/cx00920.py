from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 22)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    recent_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

derived = (
    with_stats
    .select(
        "*",
        F.concat(F.col("closed_at_label"), F.lit("#"), F.col("ticket_count").cast("string")).alias("closed_at_label_ref"),
        F.abs(F.col("ticket_count") - 5).alias("ticket_count_distance"),
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

joined_agent = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.closed_at_label",
        "s.closed_at_label_ref",
        "s.channel_prefix",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    joined_agent
    .groupBy("agent_id", "agent_hired_on")
    .pivot("channel", ["chat", "web", "email"])
    .agg(F.sum("requester_id"))
    .orderBy("agent_id", "agent_hired_on")
)
