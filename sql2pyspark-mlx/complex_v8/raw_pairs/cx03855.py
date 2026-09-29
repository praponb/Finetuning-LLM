from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

line_level_events = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

scored = (
    line_level_events
    .select(
        "*",
        F.coalesce(F.col("event_actor"), F.lit("UNKNOWN")).alias("event_actor_or_unknown"),
        F.length(F.trim(F.col("event_actor"))).alias("event_actor_len"),
    )
)

with_details_event = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.ticket_id").isNotNull())
    .select("s.*", F.col("t.ticket_id").alias("event_ticket_id"))
)

result = (
    with_details_event
    .cube("event_actor", "event_actor_or_unknown")
    .agg(F.sum("event_actor_len").alias("total_event_actor_len"))
    .select(
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        F.coalesce(F.col("event_actor_or_unknown"), F.lit("ALL")).alias("event_actor_or_unknown"),
        "total_event_actor_len",
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("event_actor_or_unknown").asc_nulls_last(),
    )
)
