from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")) & (F.col("t.event_type").isin("created", "replied", "assigned")), "left_semi")
)

annotated = (
    with_activity
    .select(
        "*",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.concat(F.col("actor_clean"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_clean_ref"),
    )
)

result = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_clean",
        "actor_ref",
        "event_type_clean",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown_2"),
    )
    .orderBy(F.col("id").asc())
)
