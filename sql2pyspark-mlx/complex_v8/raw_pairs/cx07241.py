from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

enriched_agent = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T2", "T1"))
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_activity = (
    enriched_agent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") < F.lit("2025-02-10 04:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("csat") > 2)
)

with_children = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

exploded_detail = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.agent_hired_on",
        "s.event_id",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

scored = (
    exploded_detail
    .select(
        "*",
        F.date_trunc("year", F.col("agent_hired_on")).alias("agent_hired_on_year"),
        F.when(F.col("event_event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_event_type") == "resolved", "group_b").otherwise("other").alias("event_event_type_group"),
    )
)

combined_details_event = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select("s.*", "t.event_type")
)

result = (
    combined_details_event
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "event_id",
        "event_id_2",
        "event_event_type",
        "event_event_at",
        F.round((F.unix_timestamp(F.col("event_event_at")) - F.unix_timestamp(F.col("agent_hired_on_year"))) / 3600, 1).alias("hours_agent_hired_on_year_to_event_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("event_actor")), " ", "_")).alias("event_actor_slug"),
    )
    .orderBy(F.col("event_actor_slug").desc(), F.col("priority").desc())
)
