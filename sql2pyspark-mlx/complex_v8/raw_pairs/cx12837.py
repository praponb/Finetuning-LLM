from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 36)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug_2"),
    )
)

joined = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") <= 8)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_slug_2",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

enriched = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_agent_id",
        "s.ticket_opened_at",
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    enriched
    .groupingSets(
        [["ticket_id", "ticket_agent_id"], ["ticket_id"], ["ticket_agent_id"], []],
        "ticket_id", "ticket_agent_id",
    )
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_agent_id").asc_nulls_last(),
    )
)
