from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("event_type") == "resolved") & (F.col("actor") != "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_group",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

qualified = (
    joined.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

enriched = (
    qualified
    .select(
        "*",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        (F.floor(F.col("days_since_event_at") / 100) * 100).alias("days_since_event_at_bucket"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_group",
        "s.ticket_agent_id",
        "s.event_type_prefix",
        "s.days_since_event_at_bucket",
        F.col("t.channel").alias("ticket_channel"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_group",
        "s.ticket_agent_id",
        "s.event_type_prefix",
        "s.ticket_channel",
        F.col("su.team").alias("agent_team"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.channel").alias("ticket_channel_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_type_group",
        "event_type_prefix",
        "ticket_channel",
        "agent_team",
        "ticket_requester_id",
        "ticket_channel_2",
        F.coalesce(F.col("ticket_channel_2"), F.lit("UNKNOWN")).alias("ticket_channel_2_or_unknown"),
    )
)
