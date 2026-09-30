from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_len",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    with_details
    .select("id", "event_type_len", "days_since_event_at")
    .unpivot(
        ids=["id"],
        values=["event_type_len", "days_since_event_at"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
