from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("created", "reopened", "escalated")) | (F.col("ticket_id") >= 8))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_rows = prepared_events.select("id")

event_rows_2 = df_ticket_events.select("ticket_id")

result = (
    event_rows
    .intersect(event_rows_2)
    .orderBy("id")
)
