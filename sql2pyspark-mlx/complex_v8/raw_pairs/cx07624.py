from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.hour(F.col("closed_at")).between(7, 9)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.channel_len",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

profiled = (
    with_details.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at", "t.first_ticket_opened_at")
)

enriched = (
    profiled
    .filter(F.month(F.col("first_ticket_opened_at")).isin(2, 3))
    .select(
        "*",
        (F.floor(F.col("channel_len") / 2) * 2).alias("channel_len_bucket"),
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
        (F.floor(F.col("channel_len") / 2) * 2).alias("channel_len_bucket_2"),
    )
)

result = (
    enriched
    .groupingSets(
        [["agent_team", "channel_len_bucket"], ["agent_team"], []],
        "agent_team", "channel_len_bucket",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("agent_team") == "VIP", F.col("channel_len")).otherwise(0)).alias("channel_len_agent_team_vip"),
    )
    .select(
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        "channel_len_bucket",
        "gid",
        "channel_len_agent_team_vip",
    )
)
