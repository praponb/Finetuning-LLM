from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-02-19 21:00:00").cast("timestamp")) & (F.array_contains(F.col("tags"), "bug")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
    )
)

joined = (
    filtered_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier").isin("T3", "T2"))
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined
    .groupingSets(
        [["agent_tier", "agent_id"], ["agent_tier"], ["agent_id"], []],
        "agent_tier", "agent_id",
    )
    .agg(
        F.max("closed_at").alias("last_closed_at"),
        F.count(F.when(~F.col("agent_tier").isin("T3", "T2"), 1)).alias("n_agent_tier_not_in_t3_t2"),
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
