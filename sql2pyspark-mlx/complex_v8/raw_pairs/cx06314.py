from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-03-14").cast("date")) | (F.col("closed_at") >= F.lit("2025-02-04 23:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
    )
)

without_matches = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "left_semi")
)

line_level_events = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.hours_closed_at_to_opened_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    line_level_events
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "hours_closed_at_to_opened_at",
        "event_id",
        "event_at",
        "event_type",
    )
    .orderBy("agent_id", "requester_id")
)
