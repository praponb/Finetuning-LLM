from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) & (F.col("event_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

filtered = (
    valid_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")), "left_anti")
)

detail_rows_events = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

combined_details_ticket = (
    detail_rows_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") < F.lit("2025-02-22 12:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_id",
        "s.event_actor",
        "s.event_event_at",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.max("opened_at").alias("last_ticket_opened_at_2"),
    )
)

profiled = (
    combined_details_ticket.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select("s.*", "t.last_ticket_opened_at", "t.last_ticket_opened_at_2")
)

result = (
    profiled
    .cube("ticket_agent_id", "ticket_csat")
    .agg(
        F.min("ticket_csat").alias("min_ticket_csat"),
        F.round(F.avg("ticket_csat"), 2).alias("avg_ticket_csat"),
    )
    .orderBy(
        F.col("ticket_agent_id").asc_nulls_last(),
        F.col("ticket_csat").asc_nulls_last(),
    )
)
