from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("actor") != "system") & (F.col("ticket_id") >= 32))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_year",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.tier").alias("agent_tier"),
    )
)

ticket_id_stats = (
    with_parent
    .groupBy("ticket_id")
    .agg(F.max("ticket_requester_id").alias("max_ticket_requester_id"))
)

above_average = (
    with_parent.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.ticket_requester_id") == F.col("g.max_ticket_requester_id"))
    .select(
        "s.*",
        "g.max_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.max_ticket_requester_id"), 2).alias("ticket_requester_id_minus_max"),
    )
)

result = (
    above_average
    .groupBy("agent_tier")
    .pivot("ticket_priority", ["P3", "P1", "P2", "P4"])
    .agg(F.sum("ticket_requester_id"))
    .orderBy("agent_tier")
)
