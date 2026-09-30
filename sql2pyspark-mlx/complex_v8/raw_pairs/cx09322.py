from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("event_type") != "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

annotated = (
    prepared
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

result = (
    annotated
    .filter(F.col("ticket_id") >= 9)
    .select(
        "id",
        "actor",
        "event_at_month",
        "event_at_plus_2h",
        F.concat_ws("-", F.col("actor_slug"), F.col("actor")).alias("actor_slug_actor_label"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug_2"),
    )
    .orderBy(F.col("actor_slug_actor_label").asc_nulls_last(), F.col("actor").desc())
)
