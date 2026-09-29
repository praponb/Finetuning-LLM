from pyspark.sql import functions as F

cleaned_adjusters = (
    df_adjusters
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

line_level = (
    cleaned_adjusters.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.team",
        "s.level",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

enriched = (
    line_level.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.status").isNotNull()), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("c.policy_id"), "left")
    .filter(F.col("c.reported_at") >= F.lit("2025-02-12 07:00:00").cast("timestamp"))
    .select(
        "s.*",
        F.col("c.reported_at").alias("claim_reported_at_2"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

claim_type_stats = (
    enriched
    .groupBy("claim_type")
    .agg(F.max("level").alias("max_level"))
)

vs_group = (
    enriched.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.level") == F.col("g.max_level"))
    .select(
        "s.*",
        "g.max_level",
        F.round(F.col("s.level") - F.col("g.max_level"), 2).alias("level_minus_max"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["claim_id", "claim_type", "claim_status"], ["claim_id"], ["claim_type"], []],
        "claim_id", "claim_type", "claim_status",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("max_level"), 2).alias("avg_max_level"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("claim_status").asc_nulls_last(),
    )
)
