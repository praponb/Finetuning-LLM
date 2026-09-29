from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

event_rows = (
    recent
    .filter(F.col("actor").isin("agent", "customer"))
    .select("ticket_id")
)

event_side = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 13) & (F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select("id")
)

result = (
    event_rows
    .subtract(event_side)
    .orderBy(F.col("ticket_id").desc())
)
