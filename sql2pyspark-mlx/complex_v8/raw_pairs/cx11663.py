from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) | (F.col("csat") <= 4))
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
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count(F.when(F.col("priority") == "P1", 1)).alias("ticket_priority_count"),
    )
)

profiled = (
    recent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

joined = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
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
        "s.first_ticket_closed_at",
        "s.ticket_priority_count",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined
    .groupingSets(
        [["channel", "priority"], ["channel"], ["priority"], []],
        "channel", "priority",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("agent_team").isNotNull(), 1)) / F.count("*"), 1).alias("pct_agent_team_is_not_null"),
        F.max("csat").alias("max_csat"),
    )
)
