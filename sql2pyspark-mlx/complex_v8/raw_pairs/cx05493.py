from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")) | (F.col("actor") == "system"))
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
    .filter((F.col("actor") == "customer") & (F.col("event_type") != "replied"))
    .select(F.col("ticket_id").alias("qty"))
)

ticket_rows = df_tickets.select(F.col("requester_id").alias("qty"))

result = (
    event_side
    .subtract(ticket_rows)
    .orderBy("qty")
)
