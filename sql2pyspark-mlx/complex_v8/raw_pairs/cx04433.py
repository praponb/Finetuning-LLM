from pyspark.sql import functions as F

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.when(F.col("requester_id") >= 23, "large").when(F.col("requester_id") >= 9, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

joined_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.csat",
        "s.priority_len",
        "s.priority_ref",
        "s.requester_id_tier",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

requester_id_tier_stats = (
    joined_agent
    .groupBy("requester_id_tier")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    joined_agent.alias("s")
    .join(requester_id_tier_stats.alias("g"), F.col("g.requester_id_tier") == F.col("s.requester_id_tier"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    outliers.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

per_group = (
    with_counts
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.sum("priority_len").alias("total_priority_len"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.percentile_approx("requester_id", 0.5).alias("median_requester_id"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    per_group
    .select(
        "agent_id",
        "cnt",
        "distinct_agent_tiers",
        "total_priority_len",
        "median_requester_id",
    )
    .orderBy(
        F.col("distinct_agent_tiers").asc_nulls_last(),
        F.col("median_requester_id").desc(),
    )
)
