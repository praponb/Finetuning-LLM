from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("holder_id") <= 10))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

detail_rows_claims = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") <= 33690.0), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.premium_adj",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

joined = (
    detail_rows_claims.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium_adj",
        "s.claim_id",
        "s.claim_type",
        "s.claim_claimed_amount",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

per_group = (
    joined
    .filter(F.col("claim_id") >= 25)
    .groupBy("claim_type", "claim_id", "holder_id")
    .agg(
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
        F.min("claim_claimed_amount").alias("min_claim_claimed_amount"),
    )
)

result = (
    per_group
    .groupBy("claim_id", "distinct_claim_ids")
    .pivot("claim_type", ["flood", "theft", "collision"])
    .agg(F.min("min_claim_claimed_amount"))
)
