from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

scored = (
    matched
    .filter(F.col("agent_id") <= 8)
    .withColumn("event_at_quarter", F.quarter(F.col("event_at")))
)

result = (
    scored
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "hours_since_open",
        "event_at_quarter",
    )
    .orderBy("priority", "event_at_quarter")
)
