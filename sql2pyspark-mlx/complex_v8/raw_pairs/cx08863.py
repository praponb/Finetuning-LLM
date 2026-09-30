from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("requester_id") > 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") != "created", 1)).alias("event_event_type_count"),
    )
)

with_stats = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority") == "P3"), "left_semi")
)

combined_details_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.first_event_at",
        "s.event_count",
        "s.event_event_type_count",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

enriched = (
    combined_details_agent
    .filter(F.col("opened_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp"))
    .select(
        "*",
        F.least(F.col("event_count"), F.col("event_event_type_count")).alias("least_event_count_event_event_type_count"),
        F.size(F.col("tags")).alias("tags_count"),
        F.length(F.trim(F.col("agent_team"))).alias("agent_team_len"),
    )
)

result = (
    enriched
    .filter(F.col("agent_team") != "VIP")
    .select("event_event_type_count", "agent_team_len", "requester_id")
    .distinct()
    .orderBy(F.col("requester_id").asc(), F.col("event_event_type_count").desc())
)
