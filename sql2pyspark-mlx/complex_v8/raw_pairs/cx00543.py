from pyspark.sql import functions as F

agent_side = (
    df_support_agents
    .filter((F.col("team") != "VIP") & (F.col("tier").isNotNull()))
    .select("id", "tier", F.lit("agent").alias("record_type"))
)

event_side = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) & (F.col("actor").isin("system", "agent")))
    .select(
        F.col("ticket_id").alias("id"),
        F.col("actor").alias("tier"),
        F.lit("event").alias("record_type"),
    )
)

result = agent_side.union(event_side)
