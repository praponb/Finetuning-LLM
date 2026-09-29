from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((~F.col("actor").isin("system", "agent")) & (F.hour(F.col("event_at")).between(10, 14)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("created", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") == "chat"), "left_semi")
)

with_parent_ticket = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

with_parent = (
    with_parent_ticket.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_group",
        "s.ticket_agent_id",
        F.col("su.tier").alias("agent_tier"),
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

result = (
    qualified
    .filter(F.col("event_type_group") == "group_b")
    .select(
        "id",
        "ticket_id",
        "agent_tier",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
    .orderBy(F.col("agent_tier").asc())
)
