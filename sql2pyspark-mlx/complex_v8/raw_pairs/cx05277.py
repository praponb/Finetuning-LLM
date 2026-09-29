from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-02-10").cast("date")) | (F.col("agent_id") >= 5))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.when(F.col("requester_id") >= 15, "large").when(F.col("requester_id") >= 8, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "refund")) & (F.col("t.closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp"))
)

scored = (
    with_activity
    .withColumn("days_since_closed_at_tier", F.when(F.col("days_since_closed_at") >= 490, "A").when(F.col("days_since_closed_at") >= 210, "B").otherwise("C"))
)

enriched = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.days_since_closed_at",
        "s.requester_id_tier",
        "s.days_since_closed_at_tier",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched
    .groupingSets(
        [["agent_id", "requester_id_tier", "days_since_closed_at_tier"], ["agent_id"], []],
        "agent_id", "requester_id_tier", "days_since_closed_at_tier",
    )
    .agg(
        F.countDistinct("days_since_closed_at_tier").alias("distinct_days_since_closed_at_tiers"),
        F.count("*").alias("row_count"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        F.coalesce(F.col("days_since_closed_at_tier"), F.lit("ALL")).alias("days_since_closed_at_tier"),
        "distinct_days_since_closed_at_tiers",
        "row_count",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("requester_id_tier").asc_nulls_last(),
        F.col("days_since_closed_at_tier").asc_nulls_last(),
    )
)
