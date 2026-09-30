from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

exploded_detail_events = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "escalated"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_len",
        "s.event_type_ref",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

labeled = (
    exploded_detail_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_ref",
        "event_id",
        "event_event_at",
        F.when(F.col("event_type_len") >= 11, "A").when(F.col("event_type_len") >= 7, "B").otherwise("C").alias("event_type_len_tier"),
        F.coalesce(F.col("event_type_len"), F.lit(0)).alias("event_type_len_filled"),
    )
)

result = (
    labeled
    .groupingSets(
        [["event_type_len_tier", "event_type_len_filled", "event_id"], ["event_type_len_tier"], ["event_type_len_filled"], []],
        "event_type_len_tier", "event_type_len_filled", "event_id",
    )
    .agg(
        F.grouping("event_type_len_tier").alias("is_total_event_type_len_tier"),
        F.max("event_event_at").alias("last_event_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
    .orderBy(
        F.col("event_type_len_tier").asc_nulls_last(),
        F.col("event_type_len_filled").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )
)
