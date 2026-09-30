from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

enriched_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        "su.agent_name",
    )
)

stats = (
    enriched_ticket
    .groupBy(
        "ticket_id",
        "ticket_priority",
        F.date_trunc("month", "ticket_closed_at").alias("ticket_closed_at_month"),
    )
    .agg(
        F.sum(F.when(F.lower(F.col("agent_name")).like("%ui%"), F.col("ticket_csat")).otherwise(0)).alias("ticket_csat_lower_agent_name_like_ui"),
        F.max("ticket_closed_at").alias("last_ticket_closed_at"),
    )
)

result = (
    stats
    .groupBy("ticket_id", "ticket_closed_at_month")
    .pivot("ticket_priority", ["P1", "P3", "P4", "P2"])
    .agg(F.sum("ticket_csat_lower_agent_name_like_ui"))
    .orderBy("ticket_id", "ticket_closed_at_month")
)
