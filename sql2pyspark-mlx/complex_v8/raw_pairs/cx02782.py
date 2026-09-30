from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("L%"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.days_since_opened_at",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

derived = (
    enriched
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "*",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.concat(F.col("agent_team"), F.lit("#"), F.col("agent_id").cast("string")).alias("agent_team_ref"),
        F.abs(F.col("days_since_opened_at") - 245).alias("days_since_opened_at_distance"),
    )
)

joined_agent = (
    derived.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.tags",
        "s.days_since_opened_at",
        "s.agent_tier",
        "s.agent_name_len",
        "s.agent_team_ref",
        "s.days_since_opened_at_distance",
        F.col("su.team").alias("agent_team_2"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") == "P2", 1)).alias("ticket_priority_count"),
    )
)

summarized_rows = (
    joined_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        "days_since_opened_at",
        "agent_tier",
        "agent_name_len",
        "agent_team_ref",
        "days_since_opened_at_distance",
        "agent_team_2",
        "agent_tier_2",
        "ticket_priority_count",
    )
    .orderBy(F.col("agent_name_len").desc(), F.col("agent_tier").desc())
)
