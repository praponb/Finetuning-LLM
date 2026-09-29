from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") != "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.agent_id").alias("ticket_agent_id"))
)

result = (
    with_parent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_agent_id",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
    .orderBy(F.col("ticket_agent_id").desc())
)
