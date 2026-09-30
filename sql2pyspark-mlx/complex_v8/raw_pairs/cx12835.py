from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

combined_details_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_hour",
        "s.event_at_date",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.tier").alias("agent_tier"),
    )
)

screened = (
    combined_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("assigned", "reopened")), "left_semi")
    .filter(F.col("event_type") != "reopened")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    screened.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    with_stats
    .cube("ticket_csat", "event_type")
    .agg(F.count("*").alias("n_rows"))
    .select(
        "ticket_csat",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "n_rows",
    )
    .orderBy(
        F.col("ticket_csat").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
