from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("ticket_id") <= 35))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

without_matches = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_semi")
)

result = (
    without_matches
    .select("id", "event_at")
    .distinct()
    .orderBy(F.col("event_at").desc())
)
