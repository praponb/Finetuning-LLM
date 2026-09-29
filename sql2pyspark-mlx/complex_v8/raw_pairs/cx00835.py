from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("csat") <= 4) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id_2"),
    )
)

joined_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

filtered = (
    joined_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Onboarding") & (F.lower(F.col("su.agent_name")).like("%at%")), "left_anti")
)

without_matches = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T2", "T1")), "left_semi")
)

result = (
    without_matches
    .groupBy("agent_id", "agent_hired_on")
    .pivot("priority", ["P3", "P1", "P2"])
    .agg(F.sum("csat_per_requester_id").alias("total"), F.max("csat_per_requester_id").alias("max_v"))
)
