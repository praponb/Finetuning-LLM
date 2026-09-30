from pyspark.sql import functions as F

filtered_events = df_ticket_events.filter(F.col("ticket_id").isNotNull())

annotated = (
    filtered_events
    .filter(F.col("event_type") == "replied")
    .withColumn("event_at_day", F.date_trunc("day", F.col("event_at")))
)

joined_ticket = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "ticket_agent_id",
        (F.col("ticket_closed_at") + F.expr("INTERVAL 7 DAYS")).alias("ticket_closed_at_plus_7d"),
    )
)
