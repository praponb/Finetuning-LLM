from pyspark.sql import functions as F

cleaned_agents = (
    df_support_agents
    .filter(F.col("agent_name").isNotNull())
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("tier")), 1, 2).alias("tier_prefix"),
    )
)

agent_side = (
    cleaned_agents
    .filter((F.col("hired_on") < F.lit("2025-02-26").cast("date")) & (F.col("team") == "VIP"))
    .select("team")
)

event_side = df_ticket_events.select("actor")

result = (
    agent_side
    .intersect(event_side)
    .orderBy("team")
)
