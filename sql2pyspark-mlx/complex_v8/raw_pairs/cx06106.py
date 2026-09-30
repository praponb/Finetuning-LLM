from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("event_type") != "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

labeled = (
    recent_events
    .select(
        "*",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

scored = (
    labeled
    .filter(F.col("days_since_event_at") > 140)
    .select(
        "id",
        "event_at",
        "actor",
        "event_type_or_unknown",
        "days_since_event_at",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.when(F.col("days_since_event_at") >= 595, "A").when(F.col("days_since_event_at") >= 245, "B").otherwise("C").alias("days_since_event_at_tier"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "actor",
        "days_since_event_at",
        "event_at_label",
        "days_since_event_at_tier",
    )
    .orderBy("event_at_label")
)
