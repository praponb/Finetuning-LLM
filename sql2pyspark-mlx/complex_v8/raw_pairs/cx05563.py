from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(10, 14)) & (F.month(F.col("closed_at")).isin(2, 3)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isNotNull(), 1)).alias("ticket_priority_count"),
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
    )
)

summarized_rows = (
    valid_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

with_details_agent = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.days_since_opened_at",
        "s.channel_priority_label",
        "s.channel_prefix",
        "s.ticket_priority_count",
        "s.ticket_channel_count",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

summary = (
    with_details_agent
    .groupBy("agent_team")
    .agg(
        F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"),
        F.max("days_since_opened_at").alias("max_days_since_opened_at"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    summary
    .groupBy("row_count")
    .pivot("agent_team", ["Onboarding", "Tech", "VIP", "Billing"])
    .agg(F.avg("n_priority_p4"))
    .orderBy("row_count")
)
