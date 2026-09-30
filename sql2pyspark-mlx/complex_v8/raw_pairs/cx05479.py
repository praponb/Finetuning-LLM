from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date")) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

derived = (
    prepared_events
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at_month")).alias("days_since_event_at_month"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.length(F.trim(F.col("event_type_or_unknown"))).alias("event_type_or_unknown_len"),
    )
)

result = (
    derived
    .groupingSets(
        [["event_type", "event_type_or_unknown"], ["event_type"], []],
        "event_type", "event_type_or_unknown",
    )
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at_month").alias("first_event_at_month"),
    )
)
