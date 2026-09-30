from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
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
    .filter(F.col("ticket_id") >= 35)
    .select("actor", F.col("id").alias("qty"), F.col("event_at").alias("event_time"))
)

event_side = df_ticket_events.select("actor", "id", "event_at")

ticket_side = df_tickets.select("channel", "agent_id", "opened_at")

combined = (
    event_rows
    .union(event_side)
    .union(ticket_side)
)

result = (
    combined
    .groupBy("actor")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("actor")
)
