from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-27 06:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

with_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-07-28").cast("date")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

exploded_detail_events = (
    with_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.agent_name",
        "s.agent_hired_on",
        "s.agent_tier",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    exploded_detail_events
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "agent_name",
        "agent_hired_on",
        "agent_tier",
        "event_id",
        "event_actor",
        "event_at",
        "event_type",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
    .orderBy(F.col("agent_id").desc())
)
