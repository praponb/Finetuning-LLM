from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.col("priority").isin("P4", "P3"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
    )
)

with_children_events = (
    base_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children_events
    .groupBy("agent_id", "event_id")
    .pivot("event_actor", ["system", "agent", "customer"])
    .agg(F.min("csat").alias("min_v"), F.sum("csat").alias("sum_v"))
)
