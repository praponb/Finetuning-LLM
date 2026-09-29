from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 57) | (F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

with_details_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_ref",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.team").alias("agent_team"),
    )
)

with_parent_agent = (
    with_details_ticket.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_parent_agent
    .select(
        "id",
        F.col("ticket_requester_id").alias("m_ticket_requester_id"),
        F.col("ticket_csat").alias("ticket_csat_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_ticket_requester_id", "ticket_csat_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
