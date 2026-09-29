from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 3) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_parent = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "login")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent
    .filter(F.col("ticket_id") <= 45)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_requester_id",
        "agent_hired_on",
        "agent_team",
        "ticket_channel",
        "ticket_agent_id",
        "ticket_tags",
    )
    .orderBy(F.col("agent_hired_on").desc_nulls_first())
)
