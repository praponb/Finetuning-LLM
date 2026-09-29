from pyspark.sql import functions as F

recent = (
    df_tickets
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
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("event_type").isin("replied", "escalated", "created"), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

filtered = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("channel") == "web")
)

combined_details = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

line_level = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.opened_at_label",
        "s.requester_id_distance",
        "s.first_event_at",
        "s.event_event_type_count",
        "s.agent_tier",
        "s.agent_team",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    line_level
    .select(
        "id",
        "event_id",
        "event_event_type_count",
        "requester_id_distance",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["event_event_type_count", "requester_id_distance"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
