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

detail_rows = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_activity = (
    detail_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") == "email") & (F.month(F.col("t.opened_at")).isin(1, 3)), "left_anti")
)

joined_ticket = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id_2"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.agent_id",
        "s.ticket_id_2",
        "s.ticket_opened_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    joined_ticket
    .filter(F.col("ticket_id_2").isNotNull())
    .select(
        "id",
        "ticket_id",
        "agent_id",
        "ticket_id_2",
        "ticket_opened_at",
        "ticket_agent_id",
        "agent_hired_on",
        "agent_team",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
    .orderBy(F.col("ticket_agent_id").desc(), "id", "ticket_id_2")
    .limit(10)
)
