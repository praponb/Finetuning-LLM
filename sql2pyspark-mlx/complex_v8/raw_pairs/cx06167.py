from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (~F.col("t.priority").isin("P4", "P2")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

labeled = (
    combined_details_ticket
    .filter(F.col("ticket_priority") == "P2")
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

result = (
    labeled
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "actor_len",
    )
    .orderBy(F.col("actor_len").desc_nulls_first(), "id")
    .limit(10)
)
