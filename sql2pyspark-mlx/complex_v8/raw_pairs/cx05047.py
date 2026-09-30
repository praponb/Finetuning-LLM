from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) & (F.col("ticket_id") >= 50))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_rows = (
    cleaned_events
    .filter((F.col("event_type").isin("assigned", "reopened")) & (F.col("actor") == "customer"))
    .select("ticket_id", F.col("event_type").alias("name"))
)

ticket_side = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(10, 15)) & (F.col("priority") == "P2"))
    .select(
        F.col("requester_id").alias("ticket_id"),
        F.col("priority").alias("name"),
    )
)

result = (
    event_rows
    .subtract(ticket_side)
    .orderBy("ticket_id")
)
