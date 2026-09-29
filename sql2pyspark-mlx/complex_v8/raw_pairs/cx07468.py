from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
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

enriched_ticket = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    enriched_ticket
    .filter(F.col("event_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "hours_since_open",
        "ticket_opened_at",
        "agent_name",
    )
    .orderBy(F.col("ticket_opened_at").desc(), "event_at")
)
