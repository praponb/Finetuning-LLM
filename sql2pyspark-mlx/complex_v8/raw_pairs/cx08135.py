from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.when(F.col("event_type").isin("replied", "escalated"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("event_at_label").isNotNull())
)

annotated = (
    without_matches
    .filter(F.to_date(F.col("event_at_week")) < F.lit("2025-01-25").cast("date"))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("event_at_week")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_event_at_week"),
    )
)

result = (
    annotated
    .cube("event_type_group", "event_type", "actor")
    .agg(
        F.max("minutes_event_at_to_event_at_week").alias("max_minutes_event_at_to_event_at_week"),
    )
    .orderBy(
        F.col("event_type_group").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
