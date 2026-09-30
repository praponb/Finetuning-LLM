from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(6, 12)) & (F.col("priority") == "P3"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
    )
)

exploded_detail_events = (
    prepared_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("escalated", "created")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    exploded_detail_events
    .groupingSets(
        [["agent_id", "priority"], ["agent_id"]],
        "agent_id", "priority",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.count(F.when(F.col("channel").isin("chat", "web", "email"), 1)).alias("n_channel_in_chat_web_email"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), F.col("priority").asc_nulls_last())
)
