from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.col("priority") != "P2") & (F.col("channel") == "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.when(F.col("requester_id") >= 22, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
    )
)

per_label = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "hours_closed_at_to_opened_at",
        "requester_id_tier",
        "closed_at_day",
        F.explode("tags").alias("tag"),
    )
)

line_level = (
    per_label.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.hours_closed_at_to_opened_at",
        "s.requester_id_tier",
        "s.tag",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

combined_details = (
    line_level.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.hours_closed_at_to_opened_at",
        "s.requester_id_tier",
        "s.tag",
        "s.event_id",
        "s.event_at",
        "s.event_type",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel").isin("chat", "phone")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tag",
        "event_id",
        "event_at",
        "event_type",
        "agent_hired_on",
    )
    .orderBy("tag", F.col("id").desc(), "event_id")
    .limit(10)
)
