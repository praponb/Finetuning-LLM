from pyspark.sql import functions as F

matched = (
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

stats = (
    matched
    .groupBy("claim_type", "status", "product")
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_claim_type_is_not_null"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claimed_amount_to_premium_ratio"),
    )
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.avg("pct_claim_type_is_not_null").alias("overall_avg_pct_claim_type_is_not_null")))
    .filter(F.col("pct_claim_type_is_not_null") > F.col("overall_avg_pct_claim_type_is_not_null"))
    .drop("overall_avg_pct_claim_type_is_not_null")
)

labeled = (
    with_activity
    .filter(~F.col("status").isin("PAID", "IN_REVIEW"))
    .select(
        "claim_type",
        "status",
        "product",
        "pct_claim_type_is_not_null",
        "claimed_amount_to_premium_ratio",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.length(F.trim(F.col("status"))).alias("status_len_2"),
    )
)

result = (
    labeled
    .select(
        "claim_type",
        "status",
        "product",
        "pct_claim_type_is_not_null",
        "claimed_amount_to_premium_ratio",
        "status_len",
        "status_len_2",
    )
    .orderBy(F.col("claim_type").desc(), "status")
)
