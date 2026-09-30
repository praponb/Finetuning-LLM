from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

joined = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_slug",
        F.col("t.priority").alias("ticket_priority"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_slug",
        "ticket_priority",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_priority")), " ", "_")).alias("ticket_priority_slug"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.coalesce(F.col("ticket_priority"), F.lit("UNKNOWN")).alias("ticket_priority_or_unknown"),
    )
)

result = (
    enriched
    .cube("ticket_id", "event_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "n_rows",
        "distinct_ids",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
