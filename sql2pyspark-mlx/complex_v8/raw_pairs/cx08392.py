from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) & (F.col("event_type") == "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.when(F.col("event_type").isin("resolved", "reopened"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

scored = (
    valid_events
    .filter(F.col("event_type_slug").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_or_unknown",
        "event_type_group",
        "event_type_slug",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.substring(F.trim(F.col("event_type_group")), 1, 2).alias("event_type_group_prefix"),
    )
)

detail_rows = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_or_unknown",
        "s.event_type_group",
        "s.event_at_label",
        "s.event_type_prefix",
        "s.event_type_group_prefix",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

annotated = (
    detail_rows
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("event_event_at")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_event_event_at"),
        F.length(F.trim(F.col("actor_or_unknown"))).alias("actor_or_unknown_len"),
    )
)

per_group = (
    annotated
    .filter(F.col("actor_or_unknown") != "system")
    .groupBy("event_type", "ticket_id", "event_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("event_type_group").alias("distinct_event_type_groups"),
        F.sum("actor_or_unknown_len").alias("total_actor_or_unknown_len"),
        F.min("event_at").alias("first_event_at"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("created", "reopened", "assigned"), 1)) / F.count("*"), 1).alias("pct_event_type_in_created_reopen"),
    )
)

result = (
    per_group
    .filter(F.col("event_id") >= 90)
    .select(
        "event_type",
        "ticket_id",
        "n_rows",
        "total_actor_or_unknown_len",
        "pct_event_type_in_created_reopen",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
    .orderBy(F.col("event_type_prefix").desc())
)
