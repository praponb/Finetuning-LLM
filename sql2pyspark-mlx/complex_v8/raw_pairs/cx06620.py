from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(9, 12)) & (~F.array_contains(F.col("tags"), "upgrade")))
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
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

enriched_agent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "su.agent_name",
    )
)

line_level = (
    enriched_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.agent_name",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

enriched_agent_2 = (
    line_level.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "Onboarding")
    .select(
        "s.id",
        "s.agent_id",
        "s.agent_name",
        "s.event_id",
        "s.event_at",
        "s.event_actor",
        "s.event_type",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_counts = (
    enriched_agent_2.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        "t.first_event_at",
    )
)

screened = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

agent_id_stats = (
    screened
    .groupBy("agent_id")
    .agg(F.percentile_approx("event_count", 0.5).alias("median_event_count"))
)

outliers = (
    screened.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.event_count") >= F.col("g.median_event_count"))
    .select(
        "s.*",
        "g.median_event_count",
        F.round(F.col("s.event_count") - F.col("g.median_event_count"), 2).alias("event_count_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "event_id",
        "median_event_count",
        "event_count_minus_median",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["median_event_count", "event_count_minus_median"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
