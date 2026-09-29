from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("replied", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_details = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_week",
        "s.actor_or_unknown",
        F.col("t.tags").alias("ticket_tags"),
    )
)

enriched = (
    with_details
    .filter(F.to_date(F.col("event_at_week")) < F.lit("2025-02-09").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_week",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_week",
        "event_type_clean",
    )
)
