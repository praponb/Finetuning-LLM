from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

exploded_detail = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.status",
        "s.claim_type",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_counts = (
    exploded_detail.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.total_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.max_claim_approved_amount",
    )
)

result = (
    with_counts
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "claim_type",
        "claim_status",
        "claim_reported_at",
        "total_claim_claimed_amount",
        "claim_count",
        "max_claim_approved_amount",
    )
    .orderBy(F.col("total_claim_claimed_amount").desc_nulls_first())
)
