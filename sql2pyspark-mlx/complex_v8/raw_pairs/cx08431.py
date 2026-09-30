from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("event_type") != "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 13)), "left_anti")
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date"))
)

combined_details = (
    screened.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_len",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

with_activity = (
    combined_details.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "resolved")), "left_anti")
    .filter(F.col("agent_team") != "Onboarding")
)

result = (
    with_activity
    .rollup("ticket_channel", "event_type_len")
    .agg(F.sum("event_type_len").alias("total_event_type_len"))
    .orderBy(
        F.col("ticket_channel").asc_nulls_last(),
        F.col("event_type_len").asc_nulls_last(),
    )
)
