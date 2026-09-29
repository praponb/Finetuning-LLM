from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 37)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = (
    scoped_events
    .filter(F.col("actor").isNotNull())
    .select("ticket_id", "event_at")
)

ticket_rows = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-18").cast("date"))
    .select("id", "closed_at")
)

result = (
    event_side
    .union(ticket_rows)
    .orderBy("event_at")
)
