from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 2))
    .select(
        F.col("csat").alias("qty"),
        F.col("closed_at").alias("event_time"),
        "channel",
    )
)

ticket_side_2 = df_tickets.select("csat", "opened_at", "channel")

event_side = df_ticket_events.select("ticket_id", "event_at", "actor")

result = (
    ticket_side
    .union(ticket_side_2)
    .union(event_side)
)
