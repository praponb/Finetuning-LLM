from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("reopened", "created")) | (F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

tagged = (
    cleaned_events
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("days_since_event_at") >= 490, "large").when(F.col("days_since_event_at") >= 140, "medium").otherwise("small").alias("days_since_event_at_tier"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        (F.floor(F.col("days_since_event_at") / 100) * 100).alias("days_since_event_at_bucket"),
    )
)

enriched = (
    tagged
    .filter(F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .select(
        "id",
        "event_type",
        "actor",
        "days_since_event_at_tier",
        "event_type_ref",
        F.concat_ws(": ", F.col("actor"), F.col("event_type_ref")).alias("actor_event_type_ref_label"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lower(F.trim(F.col("event_type_ref"))).alias("event_type_ref_clean"),
        F.substring(F.trim(F.col("days_since_event_at_tier")), 1, 4).alias("days_since_event_at_tier_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type_ref")), " ", "_")).alias("event_type_ref_slug"),
    )
)

enriched_2 = (
    scored
    .filter(F.col("days_since_event_at_tier_prefix").isNotNull())
    .select(
        "*",
        F.when(F.col("days_since_event_at_tier").isin("medium", "large"), "group_a").when(F.col("days_since_event_at_tier") == "small", "group_b").otherwise("other").alias("days_since_event_at_tier_group"),
        F.coalesce(F.col("days_since_event_at_tier"), F.lit("UNKNOWN")).alias("days_since_event_at_tier_or_unknown"),
        F.concat_ws("-", F.col("days_since_event_at_tier"), F.col("event_type")).alias("days_since_event_at_tier_event_type_label"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_ref",
        "actor_event_type_ref_label",
        "event_type_ref_clean",
        "event_type_ref_slug",
        "days_since_event_at_tier_group",
        "days_since_event_at_tier_or_unknown",
    )
    .orderBy(F.col("event_type_ref").desc(), "id")
    .limit(5)
)
