from pyspark.sql import functions as F

ticket_side = df_tickets.select("channel", "agent_id", "priority")

event_rows = df_ticket_events.select("event_type", "ticket_id", "actor")

result = (
    ticket_side
    .union(event_rows)
    .orderBy("channel", "priority")
)
