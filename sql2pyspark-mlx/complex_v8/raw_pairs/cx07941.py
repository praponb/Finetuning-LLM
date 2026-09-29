from pyspark.sql import functions as F

in_window = (
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
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("claim_claim_type_count"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.max_claim_claimed_amount",
        "c.avg_claim_claimed_amount",
    )
)

enriched = (
    with_stats
    .select(
        "*",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") >= 2887.3), "left_semi")
)

result = (
    qualified
    .groupBy("claim_date")
    .pivot("product", ["HOME", "AUTO", "TRAVEL", "HEALTH"])
    .agg(F.min("avg_claim_claimed_amount"))
)
