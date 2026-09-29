from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_side = filtered_events.select(F.col("event_type").alias("label"), "id")

agent_side = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2025-02-26").cast("date"))
    .select(F.col("agent_name").alias("label"), "id")
)

result = event_side.unionByName(agent_side)
