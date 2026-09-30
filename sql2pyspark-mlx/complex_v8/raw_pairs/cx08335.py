from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((~F.col("event_type").isin("escalated", "resolved")) | (F.col("ticket_id") >= 27))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.team").alias("agent_team"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "resolved", "assigned")), "left_anti")
)

result = (
    with_activity
    .select("id", "ticket_csat", "days_since_event_at")
    .unpivot(
        ids=["id"],
        values=["ticket_csat", "days_since_event_at"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
