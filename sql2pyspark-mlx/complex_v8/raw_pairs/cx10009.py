from pyspark.sql import functions as F

covered = (
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

scored = (
    covered
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isNotNull()), "left_semi")
)

result = (
    qualified
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "priority",
        "agent_id",
        "priority_len",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
