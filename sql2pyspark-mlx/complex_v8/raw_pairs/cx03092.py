from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.concat_ws(" / ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

enriched_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.year(F.col("su.hired_on")) == 2024), "inner")
    .filter(F.col("su.tier") != "T3")
    .select("s.*", F.col("su.tier").alias("agent_tier"))
)

detail_rows_events = (
    enriched_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isNotNull()), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.priority_len",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

joined_event = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.event_id",
        "s.event_at",
        F.col("t.event_at").alias("event_event_at"),
    )
)

with_parent_event = (
    joined_event.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.event_type") == "reopened")
    .select("s.*", "t.event_type")
)

result = (
    with_parent_event
    .filter(F.col("event_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "event_at",
        "event_event_at",
        "event_type",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
    .orderBy(F.col("id").asc(), F.col("event_at").desc())
)
