from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 23)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("reopened", "assigned")) & (F.col("t.event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
    .orderBy("event_at", F.col("actor").desc_nulls_first(), "id")
    .limit(5)
)
