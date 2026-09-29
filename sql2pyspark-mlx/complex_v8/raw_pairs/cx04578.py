from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean_2"),
    )
)

without_matches = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(9, 14)) & (F.col("t.event_type").isNotNull()), "left_anti")
)

joined_ticket = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "login")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.hour(F.col("t.opened_at")).between(9, 14))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_clean_2",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_clean_2",
        "ticket_channel",
        "ticket_opened_at",
        "agent_tier",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
    .orderBy(F.col("ticket_opened_at").desc(), F.col("agent_tier").asc())
)
