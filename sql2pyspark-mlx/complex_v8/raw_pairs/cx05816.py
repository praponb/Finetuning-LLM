from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 12))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    valid
    .filter((F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("ticket_id") >= 17))
    .select(F.col("ticket_id").alias("entity_id"))
)

event_rows = df_ticket_events.select(F.col("id").alias("entity_id"))

result = (
    event_side
    .subtract(event_rows)
    .orderBy("entity_id")
)
