from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "agent") & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.when(F.col("event_type").isin("escalated", "assigned"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.actor_event_type_label",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

enriched_agent = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.ticket_agent_id")) & (F.col("su.hired_on").isNotNull()), "inner")
    .filter(F.col("su.tier") != "T3")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_group",
        "s.actor_event_type_label",
        "s.ticket_channel",
        "s.ticket_agent_id",
        "s.ticket_closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

with_parent_ticket = (
    enriched_agent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_event_type_label",
        "s.ticket_agent_id",
        "s.ticket_closed_at",
        "s.agent_hired_on",
        "s.agent_tier",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_event_type_label",
        "ticket_agent_id",
        "ticket_closed_at",
        "agent_hired_on",
        "agent_tier",
        "ticket_opened_at",
        (F.col("ticket_closed_at_2") + F.expr("INTERVAL 30 MINUTES")).alias("ticket_closed_at_2_plus_30m"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
    .orderBy(F.col("ticket_closed_at_2_plus_30m").desc())
)
