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

policy_id_stats = (
    joined_ranges
    .groupBy("policy_id")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

outliers = (
    joined_ranges.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "product",
        "avg_claimed_amount",
        "claimed_amount_minus_avg",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
    .orderBy("premium_filled", F.col("claim_date").desc())
)
