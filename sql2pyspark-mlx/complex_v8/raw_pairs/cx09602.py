from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("customer", "system")) & (F.col("t.event_type") == "escalated"), "left_semi")
)

with_details = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_or_unknown",
        "s.actor_prefix",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    with_details
    .filter(F.col("actor_or_unknown") != "UNKNOWN")
    .select(
        "id",
        "event_at",
        "actor_prefix",
        "ticket_tags",
        "ticket_agent_id",
    )
    .orderBy(F.col("event_at").asc_nulls_last(), "id")
    .limit(10)
)
