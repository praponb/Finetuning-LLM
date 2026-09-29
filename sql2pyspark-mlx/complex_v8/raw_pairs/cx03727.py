from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "refund"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

exploded_detail_events = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_or_unknown",
        "s.opened_at_quarter",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

tagged = (
    exploded_detail_events
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "priority_or_unknown",
        "opened_at_quarter",
        "event_id",
        "event_at",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

detail_rows_events = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.opened_at_quarter",
        "s.event_id",
        "s.event_at",
        F.col("t.id").alias("event_id_2"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

per_label = (
    detail_rows_events
    .select(
        "id",
        "agent_id",
        "channel",
        "opened_at_quarter",
        "event_id",
        "event_at",
        "event_id_2",
        "event_event_at",
        "event_actor",
        F.explode("tags").alias("tag"),
    )
)

enriched = (
    per_label.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id_2"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.event_id",
        "s.event_at",
        "s.event_id_2",
        "s.event_event_at",
        "s.tag",
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_at").alias("event_event_at_2"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    enriched
    .select(
        "id",
        "agent_id",
        "event_at",
        "event_id_2",
        "tag",
        "event_actor_2",
        "agent_tier",
        "agent_name",
        F.date_format(F.col("event_event_at_2"), "yyyy-MM").alias("event_event_at_2_label"),
    )
    .orderBy(F.col("agent_id").desc())
)
