from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(8, 13)), "left_semi")
)

result = (
    with_activity
    .select("id", "ticket_id", "event_at")
    .orderBy(F.col("ticket_id").desc_nulls_first())
)
