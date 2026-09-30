from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

event_rows = (
    filtered
    .select(
        F.col("ticket_id").alias("entity_id"),
        F.col("id").alias("qty"),
        "event_at",
    )
)

event_side = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 14))
    .select(
        F.col("id").alias("entity_id"),
        F.col("ticket_id").alias("qty"),
        "event_at",
    )
)

result = event_rows.unionByName(event_side)
