from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-15 08:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_parent = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

rollup_base = (
    with_parent
    .groupBy("csat", "priority", "channel")
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("agent_tier") == "T3", 1)) / F.count("*"), 1).alias("pct_agent_tier_t3"),
        F.min("csat").alias("min_csat"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .filter(F.col("pct_agent_tier_t3") < 60.0)
    .select(
        "csat",
        "priority",
        "channel",
        "row_count",
        "pct_agent_tier_t3",
        "min_csat",
    )
)
