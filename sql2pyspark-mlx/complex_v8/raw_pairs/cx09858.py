from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("csat") >= 5, "HIGH").when(F.col("csat") >= 2, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

qualified = (
    scoped_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id").between(2, 23)), "left_semi")
)

enriched_agent = (
    qualified.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T2", "T1"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.csat_tier",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    enriched_agent
    .groupingSets(
        [["channel", "csat_tier", "csat"], ["channel"]],
        "channel", "csat_tier", "csat",
    )
    .agg(
        F.grouping("channel").alias("is_total_channel"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
        F.min("csat").alias("min_csat"),
    )
    .orderBy(
        F.col("channel").asc_nulls_last(),
        F.col("csat_tier").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
    )
)
