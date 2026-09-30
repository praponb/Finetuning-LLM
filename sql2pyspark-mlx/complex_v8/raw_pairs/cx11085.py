from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
)

enriched = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") < 21), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["agent_tier", "actor", "event_type"], ["agent_tier"], []],
        "agent_tier", "actor", "event_type",
    )
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("n_rows"),
        F.min("agent_hired_on").alias("first_agent_hired_on"),
    )
    .select(
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "last_event_at",
        "n_rows",
        "first_agent_hired_on",
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
