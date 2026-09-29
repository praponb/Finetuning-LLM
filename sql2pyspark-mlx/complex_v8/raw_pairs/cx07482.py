from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.hour(F.col("event_at")).between(6, 10)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.id", "s.ticket_id", F.col("t.agent_id").alias("ticket_agent_id"))
)

result = (
    with_details
    .select("id", "ticket_id", "ticket_agent_id")
    .orderBy(F.col("ticket_id").asc_nulls_last(), "id")
    .limit(20)
)
