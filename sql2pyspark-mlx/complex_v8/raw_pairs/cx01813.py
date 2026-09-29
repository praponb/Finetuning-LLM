from pyspark.sql import functions as F

ticket_rows = df_tickets.select("agent_id")

event_rows = (
    df_ticket_events
    .filter(~F.col("actor").isin("agent", "system"))
    .select("ticket_id")
)

result = (
    ticket_rows
    .exceptAll(event_rows)
    .orderBy("agent_id")
)
