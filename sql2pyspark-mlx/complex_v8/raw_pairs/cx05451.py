from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P2", "P1", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        (F.col("opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("opened_at_plus_15m"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_parent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.tags",
        "s.opened_at_plus_15m",
        "s.channel_priority_label",
        F.col("su.tier").alias("agent_tier"),
    )
)

agent_tier_stats = (
    with_parent
    .groupBy("agent_tier")
    .agg(F.percentile_approx("requester_id", 0.5).alias("median_requester_id"))
)

vs_group = (
    with_parent.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.requester_id") >= F.col("g.median_requester_id"))
    .select(
        "s.*",
        "g.median_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.median_requester_id"), 2).alias("requester_id_minus_median"),
    )
)

detail_rows = (
    vs_group.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.opened_at_plus_15m",
        "s.channel_priority_label",
        "s.median_requester_id",
        "s.requester_id_minus_median",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "opened_at_plus_15m",
        "requester_id_minus_median",
        "event_id",
        "event_actor",
        "event_at",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
    .orderBy(F.col("days_since_event_at").desc(), "id", "event_id")
    .limit(5)
)
