from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    base
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_type",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left_anti")
)

annotated = (
    without_matches
    .select(
        "*",
        F.substring(F.trim(F.col("actor_slug")), 1, 3).alias("actor_slug_prefix"),
        F.substring(F.trim(F.col("actor_slug")), 1, 4).alias("actor_slug_prefix_2"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent") & (F.col("t.event_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_slug",
        "actor_slug_prefix_2",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.concat_ws("|", F.col("event_type"), F.col("actor_slug")).alias("event_type_actor_slug_label"),
    )
    .orderBy(
        F.col("event_type_actor_slug_label").desc_nulls_first(),
        F.col("event_type_or_unknown").desc(),
    )
)
