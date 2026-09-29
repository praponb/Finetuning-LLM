from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("requester_id") <= 20)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

channel_stats = (
    cleaned
    .groupBy("channel")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

above_average = (
    cleaned.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

joined_agent = (
    above_average.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.requester_id_per_csat",
        "s.avg_requester_id",
        "s.requester_id_minus_avg",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    joined_agent
    .select(
        "id",
        F.col("avg_requester_id").alias("avg_requester_id_v"),
        F.col("requester_id_minus_avg").alias("m_requester_id_minus_avg"),
        F.col("requester_id_per_csat").alias("requester_id_per_csat_v"),
    )
    .unpivot(
        ids=["id"],
        values=["avg_requester_id_v", "m_requester_id_minus_avg", "requester_id_per_csat_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
