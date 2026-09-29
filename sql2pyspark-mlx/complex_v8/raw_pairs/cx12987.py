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

enriched = (
    in_window.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

detail_rows_tickets = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        "s.ticket_agent_id",
        "s.ticket_requester_id",
        "s.agent_name",
        "s.agent_tier",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id_2"),
    )
)

result = (
    detail_rows_tickets
    .select(
        "id",
        "ticket_id",
        "ticket_requester_id",
        "agent_tier",
        "ticket_channel",
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
    )
    .orderBy("agent_tier", F.col("ticket_id").asc_nulls_last())
)
