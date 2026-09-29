from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(10, 14))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

combined_details_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.days_since_opened_at",
        "s.priority_clean",
        F.col("su.team").alias("agent_team"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor").isin("system", "agent"), 1)).alias("event_actor_count"),
    )
)

profiled = (
    combined_details_agent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["agent_id", "agent_team"], ["agent_id"], []],
        "agent_id", "agent_team",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
