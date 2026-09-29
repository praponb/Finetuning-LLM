from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 33) | (F.col("event_type") == "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

combined_details = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.csat").alias("ticket_csat"))
)

result = (
    combined_details
    .groupBy("event_type")
    .pivot("actor", ["customer", "system"])
    .agg(F.avg("ticket_csat"))
)
