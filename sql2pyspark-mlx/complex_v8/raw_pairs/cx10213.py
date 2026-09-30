from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter((F.col("event_type") != "escalated") & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)

event_side = (
    cleaned
    .filter(F.hour(F.col("event_at_day")).between(9, 14))
    .select(
        F.lit("event").alias("origin"),
        F.col("id").alias("entity_id"),
        F.col("actor").alias("name"),
    )
)

ticket_side = (
    df_tickets
    .filter(F.col("requester_id") >= 8)
    .select(
        F.lit("ticket").alias("origin"),
        F.col("requester_id").alias("entity_id"),
        F.col("priority").alias("name"),
    )
)

agent_side = (
    df_support_agents
    .filter((F.col("team") == "Onboarding") & (F.upper(F.trim(F.col("agent_name"))).like("K%")))
    .select(
        F.lit("agent").alias("origin"),
        F.col("id").alias("entity_id"),
        F.col("team").alias("name"),
    )
)

result = (
    event_side
    .unionByName(ticket_side)
    .unionByName(agent_side)
)
