from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("requester_id") <= 16)
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
)

enriched_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-07-09").cast("date")), "inner")
    .select("s.*", F.col("su.tier").alias("agent_tier"), "su.agent_name")
)

with_details = (
    enriched_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "s.csat_distance",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    with_details
    .select(
        "id",
        "priority",
        "csat_distance",
        "agent_tier_2",
        "agent_hired_on",
    )
    .orderBy(F.col("agent_hired_on").desc(), F.col("agent_tier_2").asc())
)
