from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

combined_details_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

scored = (
    combined_details_agent
    .filter(F.month(F.col("closed_at")).isin(2, 3))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "agent_hired_on",
        F.size(F.col("tags")).alias("tags_count"),
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.min("tags_count").alias("overall_min_tags_count")))
    .filter(F.col("tags_count") > F.col("overall_min_tags_count"))
    .drop("overall_min_tags_count")
)

with_details = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.agent_hired_on",
        "s.tags_count",
        "s.closed_at_week",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

agent_tier_stats = (
    with_details
    .groupBy("agent_tier")
    .agg(F.avg("tags_count").alias("avg_tags_count"))
)

vs_group = (
    with_details.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.tags_count") > F.col("g.avg_tags_count"))
    .select(
        "s.*",
        "g.avg_tags_count",
        F.round(F.col("s.tags_count") - F.col("g.avg_tags_count"), 2).alias("tags_count_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "closed_at",
        "tags",
        "agent_hired_on",
        "tags_count",
        "closed_at_week",
        "agent_tier",
        "avg_tags_count",
        "tags_count_minus_avg",
        (F.col("closed_at") + F.expr("INTERVAL 1 DAYS")).alias("closed_at_plus_1d"),
        F.when(F.col("tags_count") >= 2, "A").when(F.col("tags_count") >= 1, "B").otherwise("C").alias("tags_count_tier"),
    )
    .orderBy(F.col("closed_at_plus_1d").desc_nulls_first())
)
