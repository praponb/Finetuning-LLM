from pyspark.sql import functions as F

in_window = (
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

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

enriched = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("t.priority") == "P2")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.priority",
        "s.agent_id",
        "s.first_event_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
    )
)

with_details_agent = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        "s.first_event_at",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_details_agent
    .select(
        "id",
        "priority",
        "agent_id",
        "first_event_at",
        "agent_team",
        "agent_hired_on",
        "agent_tier",
        "ticket_tags",
        "ticket_channel",
        F.upper(F.trim(F.col("agent_team"))).alias("agent_team_clean"),
        F.length(F.trim(F.col("ticket_channel"))).alias("ticket_channel_len"),
    )
    .orderBy("id", F.col("agent_team").asc())
)
