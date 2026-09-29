from pyspark.sql import functions as F

recent_events = df_ticket_events

event_side = recent_events.select("ticket_id")

ticket_side = (
    df_tickets
    .filter(F.col("csat") < 5)
    .select(F.col("agent_id").alias("ticket_id"))
)

result = (
    event_side
    .subtract(ticket_side)
    .orderBy(F.col("ticket_id").desc())
)
