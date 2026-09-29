from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date")))
)

combined_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

enriched = (
    combined_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(F.col("event_type_len").desc(), "id")
    .limit(10)
)
