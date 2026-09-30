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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("claimed_amount").alias("min_claim_claimed_amount"))
)

with_stats = (
    joined_ranges.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.min_claim_claimed_amount")
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_counts = (
    with_stats.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        "c.total_claim_approved_amount",
        "c.last_claim_date",
    )
)

result = (
    with_counts
    .groupBy("first_claim_reported_at", "product")
    .pivot("status", ["PAID", "APPROVED", "OPEN"])
    .agg(F.sum("total_claim_approved_amount").alias("amount"), F.count("total_claim_approved_amount").alias("n"))
    .orderBy("first_claim_reported_at", "product")
)
