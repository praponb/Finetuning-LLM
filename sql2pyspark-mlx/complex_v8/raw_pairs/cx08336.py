from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

with_details_ticket = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .filter(F.col("t.channel").isNotNull())
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

result = (
    with_details_ticket
    .filter(F.col("event_type") != "resolved")
    .select(
        "id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "ticket_channel",
        "agent_team",
        "agent_name",
    )
    .orderBy(F.col("ticket_channel").desc(), F.col("event_type").desc())
)
