from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 50)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

scored = (
    base_events
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_actor_label",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

derived = (
    scored
    .filter(F.col("actor_slug").isNotNull())
    .withColumn("event_type_actor_label_prefix", F.substring(F.trim(F.col("event_type_actor_label")), 1, 4))
)

scored_2 = (
    derived
    .select(
        "*",
        F.initcap(F.trim(F.col("event_type_actor_label"))).alias("event_type_actor_label_clean"),
    )
)

enriched = (
    scored_2
    .select(
        "id",
        "actor",
        "event_type_actor_label",
        "actor_slug",
        "event_type_actor_label_prefix",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type_actor_label")), " ", "_")).alias("event_type_actor_label_slug"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

result = (
    enriched
    .filter(F.col("event_type_or_unknown").isin("resolved", "reopened", "created"))
    .select(
        "id",
        "event_type_actor_label",
        "actor_slug",
        "event_type_actor_label_prefix",
        "event_type_or_unknown",
        "event_type_actor_label_slug",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown_2"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug_2"),
    )
    .orderBy(
        F.col("actor_or_unknown_2").desc_nulls_first(),
        F.col("actor_slug").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
