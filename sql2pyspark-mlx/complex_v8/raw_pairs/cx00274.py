from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.when(F.col("channel").isin("email", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

screened = (
    base.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp")) & (F.col("t.channel").isNotNull()), "left_anti")
    .filter(F.col("days_since_closed_at") < 560)
)

with_parent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isin("Onboarding", "Tech")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.days_since_closed_at",
        "s.channel_group",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["priority", "channel_group"], ["priority"], ["channel_group"], []],
        "priority", "channel_group",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("channel_group").asc_nulls_last(),
    )
)
