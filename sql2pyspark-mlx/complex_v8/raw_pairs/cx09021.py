from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.col("priority") == "P4") & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.max("csat").alias("overall_max_csat")))
    .filter(F.col("csat") < F.col("overall_max_csat"))
    .drop("overall_max_csat")
)

enriched_agent = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_priority_label",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    enriched_agent
    .groupBy("agent_id")
    .pivot("priority", ["P2", "P4"])
    .agg(F.max("csat"))
    .orderBy("agent_id")
)
