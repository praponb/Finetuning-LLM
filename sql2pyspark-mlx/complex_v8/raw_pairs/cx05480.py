from pyspark.sql import functions as F

ticket_rows = df_tickets.select("priority")

event_side = (
    df_ticket_events
    .filter(F.col("event_type") == "created")
    .select("actor")
)

result = (
    ticket_rows
    .intersect(event_side)
    .orderBy("priority")
)
