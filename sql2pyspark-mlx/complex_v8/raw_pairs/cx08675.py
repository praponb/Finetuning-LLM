from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

scored = cleaned.withColumn("event_type_len", F.length(F.trim(F.col("event_type"))))

with_details = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_counts = (
    with_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

with_details_ticket = (
    with_counts.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

result = (
    with_details_ticket
    .select(
        "id",
        "event_at",
        "ticket_requester_id",
        "last_event_at_2",
        "ticket_closed_at",
        "ticket_tags",
        "ticket_agent_id",
        "agent_team",
    )
    .orderBy(F.col("agent_team").asc(), "id")
    .limit(5)
)
