from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    recent_events
    .filter((F.col("actor").isin("agent", "system")) & (~F.col("event_type").isin("reopened", "resolved")))
    .select("id")
)

ticket_rows = (
    df_tickets
    .filter((F.col("csat") > 2) & (F.col("closed_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select("id")
)

result = event_side.intersect(ticket_rows)
