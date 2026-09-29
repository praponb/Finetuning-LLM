from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

tagged = (
    filtered
    .select(
        "id",
        "actor",
        "event_type_prefix",
        "actor_prefix",
        F.concat(F.col("actor_prefix"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_prefix_ref"),
    )
)

annotated = (
    tagged
    .filter(F.col("actor_prefix_ref").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_prefix")), " ", "_")).alias("event_type_prefix_slug"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        "actor",
        "event_type_prefix",
        "actor_prefix",
        "actor_prefix_ref",
        F.substring(F.trim(F.col("event_type_prefix_slug")), 1, 4).alias("event_type_prefix_slug_prefix"),
    )
    .orderBy(F.col("actor_prefix_ref").asc_nulls_last())
)
