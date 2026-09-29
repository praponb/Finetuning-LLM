from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "assigned")) & (F.col("actor") != "customer"))
)

with_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

enriched = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_closed_at",
        "agent_tier",
        "agent_team",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("ticket_closed_at"))) / 3600, 1).alias("hours_ticket_closed_at_to_event_at"),
    )
)

result = (
    enriched
    .rollup("actor", "agent_team", "agent_tier")
    .agg(
        F.round(F.avg("hours_ticket_closed_at_to_event_at"), 2).alias("avg_hours_ticket_closed_at_to_event_at"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
