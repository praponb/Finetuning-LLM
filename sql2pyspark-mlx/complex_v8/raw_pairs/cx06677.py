from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_parent = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_clean",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

result = (
    with_parent
    .groupBy("ticket_closed_at", "ticket_id")
    .pivot("event_type", ["reopened", "created", "resolved"])
    .agg(F.sum("ticket_requester_id"))
)
