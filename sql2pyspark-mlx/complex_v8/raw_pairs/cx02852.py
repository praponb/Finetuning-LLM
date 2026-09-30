from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

filtered = (
    matched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isin("P2", "P4", "P1")) & (F.col("t.csat") >= 3), "left_anti")
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    with_parent
    .filter(F.col("agent_tier") == "T1")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "hours_since_open",
        "agent_name",
        "agent_tier",
        "ticket_tags",
    )
    .orderBy(F.col("ticket_id").asc_nulls_last(), "id")
    .limit(5)
)
