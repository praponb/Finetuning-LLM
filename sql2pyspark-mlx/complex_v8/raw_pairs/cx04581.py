from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("ticket_id").isNotNull()))
    .select(
        F.lit("event").alias("origin"),
        F.col("actor").alias("name"),
        "event_type",
    )
)

agent_side = (
    df_support_agents
    .filter(F.col("hired_on").between(F.lit("2024-07-09").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(F.lit("agent").alias("origin"), "team", "agent_name")
)

result = (
    event_side
    .union(agent_side)
    .orderBy(F.col("origin").desc(), "name")
)
