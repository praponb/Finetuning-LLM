from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.ticket_priority",
        "s.ticket_agent_id",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.priority").alias("ticket_priority_2"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

enriched_2 = (
    detail_rows
    .filter(F.col("ticket_priority_2") == "P2")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_priority",
        "ticket_agent_id",
        "ticket_id_2",
        "ticket_priority_2",
        "ticket_csat",
        F.when(F.col("ticket_csat") >= 4, "HIGH").when(F.col("ticket_csat") >= 2, "MEDIUM").otherwise("LOW").alias("ticket_csat_tier"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_priority",
        "ticket_agent_id",
        "ticket_id_2",
        "ticket_csat_tier",
        "days_since_event_at",
        F.when(F.col("ticket_priority").isin("P2", "P3"), "group_a").when(F.col("ticket_priority") == "P4", "group_b").otherwise("other").alias("ticket_priority_group"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
    .orderBy("id", F.col("ticket_priority_group").desc())
)
