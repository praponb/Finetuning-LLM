from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(~F.col("event_type").isin("escalated", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

enriched = (
    base
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_type_clean",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

combined_details_ticket = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_clean",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    combined_details_ticket
    .groupBy("ticket_closed_at")
    .pivot("agent_tier", ["T2", "T3", "T1"])
    .agg(F.min("ticket_csat"))
    .orderBy("ticket_closed_at")
)
