from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "created", "reopened")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_actor_label",
        "s.event_at_day",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_day",
        "agent_tier",
        "agent_hired_on",
        F.concat_ws("|", F.col("event_type_actor_label"), F.col("event_type")).alias("event_type_actor_label_event_type_label"),
        (F.col("ticket_closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("ticket_closed_at_plus_30m"),
    )
    .orderBy("event_type_actor_label_event_type_label", "agent_tier")
)
