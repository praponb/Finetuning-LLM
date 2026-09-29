from pyspark.sql import functions as F

cleaned = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"),
    )
)

agent_side = (
    cleaned
    .filter((~F.col("tier").isin("T3", "T1")) & (F.col("team_prefix").isNotNull()))
    .select(
        F.col("id").alias("entity_id"),
        F.col("team_prefix").alias("category"),
        F.lit("agent").alias("source"),
    )
)

event_side = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select("ticket_id", "event_type", F.lit("event").alias("source"))
)

result = (
    agent_side
    .union(event_side)
    .orderBy(F.col("source").desc())
)
