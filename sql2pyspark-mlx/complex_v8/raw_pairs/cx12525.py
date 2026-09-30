from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(2, 3)) & (F.hour(F.col("opened_at")).between(7, 10)))
    .select(F.col("channel").alias("label"), "opened_at")
)

event_rows = (
    df_ticket_events
    .filter(F.col("event_type").isin("resolved", "replied"))
    .select("actor", "event_at")
)

event_side = (
    df_ticket_events
    .filter(~F.col("event_type").isin("escalated", "reopened"))
    .select("event_type", "event_at")
)

result = (
    ticket_side
    .union(event_rows)
    .union(event_side)
    .orderBy("label", "opened_at")
)
