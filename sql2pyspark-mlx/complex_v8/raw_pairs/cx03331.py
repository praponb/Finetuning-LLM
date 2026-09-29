from pyspark.sql import functions as F

covered = (
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

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("flood", "delay"), 1)).alias("claim_claim_type_count"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

profiled = (
    covered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.total_claim_claimed_amount",
    )
)

result = (
    profiled
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "status",
        "claim_type",
        "product",
        "premium",
        "claim_claim_type_count",
        "total_claim_claimed_amount",
    )
    .orderBy(F.col("status").desc())
)
