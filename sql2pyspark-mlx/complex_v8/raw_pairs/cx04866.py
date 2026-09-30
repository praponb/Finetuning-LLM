from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    covered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "premium",
        "min_claim_claimed_amount",
        "claim_claim_type_count",
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
        F.least(F.col("min_claim_claimed_amount"), F.col("premium")).alias("least_min_claim_claimed_amount_premium"),
    )
    .orderBy(
        F.col("approved_amount_adj").desc(),
        F.col("premium").desc_nulls_first(),
    )
)
