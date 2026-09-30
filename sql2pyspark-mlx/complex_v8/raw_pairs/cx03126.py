from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") == "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

enriched = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(5, 20)), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_dayofweek",
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "customer"), "left_anti")
)

with_parent_ticket = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_dayofweek",
        "s.agent_name",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    with_parent_ticket
    .withColumn("agent_hired_on_week", F.date_trunc("week", F.col("agent_hired_on")))
)

with_details = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.ticket_opened_at_2",
        "s.ticket_channel",
        "s.agent_hired_on",
        "s.agent_hired_on_week",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    with_details
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_opened_at_2",
        "ticket_channel",
        "agent_hired_on",
        "agent_hired_on_week",
        "ticket_csat",
        "ticket_tags",
        "ticket_channel_2",
        "agent_agent_name",
    )
    .orderBy(F.col("agent_hired_on_week").desc(), "id")
    .limit(20)
)
