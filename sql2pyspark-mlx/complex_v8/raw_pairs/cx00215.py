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

with_details_ticket = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "login"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
    )
)

with_details_ticket_2 = (
    with_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.agent_id",
        "s.hours_since_open",
        "s.ticket_csat",
        "s.ticket_opened_at",
        "s.ticket_tags",
        "s.agent_team",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

line_level = (
    with_details_ticket_2.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.agent_id",
        "s.ticket_csat",
        "s.ticket_opened_at",
        "s.ticket_opened_at_2",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    line_level
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "agent_id",
        "ticket_csat",
        "ticket_opened_at_2",
        "agent_hired_on",
        "event_id",
        "event_event_at",
        "event_actor",
        F.year(F.col("event_event_at")).alias("event_event_at_year"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last())
)
