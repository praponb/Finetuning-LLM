from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.opened_at")).isin(1, 3)), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_event_type_label",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_parent_ticket = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_event_type_label",
        "s.ticket_requester_id",
        "s.ticket_opened_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_event_type_label",
        "ticket_opened_at",
        "ticket_agent_id",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("ticket_opened_at")).alias("days_since_ticket_opened_at"),
    )
    .orderBy(F.col("ticket_id").desc(), "id")
    .limit(20)
)
