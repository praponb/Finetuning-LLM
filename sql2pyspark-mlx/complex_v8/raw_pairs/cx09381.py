from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

event_side = recent.select(F.col("event_type").alias("name"))

agent_side = df_support_agents.select(F.col("tier").alias("name"))

result = (
    event_side
    .intersect(agent_side)
    .orderBy("name")
)
