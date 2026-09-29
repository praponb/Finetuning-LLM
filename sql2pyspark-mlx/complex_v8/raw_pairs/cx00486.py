from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.hour(F.col("event_at")).between(8, 12)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "replied"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label_2"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") < F.lit("2025-03-08 23:00:00").cast("timestamp")), "left_semi")
)

qualified = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

with_parent_ticket = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.agent_id") <= 5)
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_group",
        "s.actor_event_type_label",
        "s.actor_event_type_label_2",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_event_type_label",
        "actor_event_type_label_2",
        "ticket_priority",
        "ticket_closed_at",
        "ticket_agent_id",
        "agent_team",
        "agent_tier",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
    .orderBy(F.col("actor_event_type_label").desc_nulls_first())
)
