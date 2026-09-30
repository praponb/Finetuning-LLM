from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(7, 12))
    .select(
        "id",
        F.col("priority").alias("label"),
        F.col("closed_at").alias("event_time"),
    )
)

event_side = (
    df_ticket_events
    .filter(~F.col("actor").isin("customer", "system"))
    .select("id", "actor", "event_at")
)

ticket_rows = (
    df_tickets
    .filter(F.col("agent_id") >= 5)
    .select("csat", "priority", "closed_at")
)

result = (
    ticket_side
    .union(event_side)
    .union(ticket_rows)
)
