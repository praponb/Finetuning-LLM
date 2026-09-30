from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(F.lit("ticket").alias("src"), "id", "csat")
)

event_side = df_ticket_events.select(F.lit("event").alias("src"), "id", "ticket_id")

ticket_rows_2 = df_tickets.select(F.lit("ticket_3").alias("src"), "id", "agent_id")

result = (
    ticket_rows
    .union(event_side)
    .union(ticket_rows_2)
)
