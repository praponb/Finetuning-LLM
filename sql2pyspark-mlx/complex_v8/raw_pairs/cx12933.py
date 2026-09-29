from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
    )
)

line_level_claims = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") <= 29359.4), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.premium_bucket",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

claim_type_stats = (
    line_level_claims
    .groupBy("claim_type")
    .agg(F.max("claim_claimed_amount").alias("max_claim_claimed_amount"))
)

vs_group = (
    line_level_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claim_claimed_amount") == F.col("g.max_claim_claimed_amount"))
    .select(
        "s.*",
        "g.max_claim_claimed_amount",
        F.round(F.col("s.claim_claimed_amount") - F.col("g.max_claim_claimed_amount"), 2).alias("claim_claimed_amount_minus_max"),
    )
)

scored = (
    vs_group
    .filter(F.col("claim_reported_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select(
        "id",
        "holder_id",
        "end_date",
        "premium_bucket",
        "claim_id",
        "claim_reported_at",
        "max_claim_claimed_amount",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.when(F.col("claim_claimed_amount") >= 30115.9, "A").when(F.col("claim_claimed_amount") >= 19609.8, "B").otherwise("C").alias("claim_claimed_amount_tier"),
    )
)

result = (
    scored
    .cube("claim_claimed_amount_tier", "holder_id")
    .agg(
        F.round(F.avg("max_claim_claimed_amount"), 2).alias("avg_max_claim_claimed_amount"),
        F.min("max_claim_claimed_amount").alias("min_max_claim_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("claim_claimed_amount_tier"), F.lit("ALL")).alias("claim_claimed_amount_tier"),
        "holder_id",
        "avg_max_claim_claimed_amount",
        "min_max_claim_claimed_amount",
    )
    .orderBy(
        F.col("claim_claimed_amount_tier").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)
