from pyspark.sql import functions as F

event_side = df_ticket_events.select("ticket_id", "event_at")

ticket_rows = (
    df_tickets
    .filter((F.col("priority").isNotNull()) & (~F.col("channel").isin("phone", "email")))
    .select("requester_id", "closed_at")
)

result = (
    event_side
    .union(ticket_rows)
    .distinct()
    .orderBy(F.col("ticket_id").desc())
)
