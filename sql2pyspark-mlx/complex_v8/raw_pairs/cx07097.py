from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
    )
)

derived = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_or_unknown",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.concat(F.col("event_type_or_unknown"), F.lit("#"), F.col("id").cast("string")).alias("event_type_or_unknown_ref"),
    )
    .orderBy(F.col("id").desc(), "event_type_or_unknown")
)
