from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

filtered = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("ticket_id").isNotNull())
)

combined_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "event_type",
        "actor",
        "ticket_agent_id",
        "agent_hired_on",
        "agent_team",
    )
    .orderBy(F.col("actor").desc(), "agent_hired_on")
)
