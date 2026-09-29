from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")) & (F.col("ticket_id") >= 56))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

combined_details = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_requester_id",
        "agent_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.round(F.col("days_since_event_at") / F.nullif(F.col("ticket_requester_id"), F.lit(0)), 3).alias("days_since_event_at_per_ticket_requester_id"),
    )
)

with_details_ticket = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 4), "inner")
    .filter(F.to_date(F.col("t.closed_at")) < F.lit("2025-02-27").cast("date"))
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id_2"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    with_details_ticket
    .select(
        "id",
        F.col("ticket_requester_id").alias("ticket_requester_id_v"),
        F.col("days_since_agent_hired_on").alias("days_since_agent_hired_on_v"),
        F.col("ticket_requester_id_2").alias("m_ticket_requester_id_2"),
    )
    .unpivot(
        ids=["id"],
        values=["ticket_requester_id_v", "days_since_agent_hired_on_v", "m_ticket_requester_id_2"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
