from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 90)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_90d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_90d"),
    )
)

with_details = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.joined_on").alias("holder_joined_on"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

labeled = (
    with_details
    .select(
        "*",
        F.coalesce(F.col("prior_rows_90d"), F.lit(0)).alias("prior_rows_90d_filled"),
        F.round(F.col("prior_claimed_amount_90d") / F.nullif(F.col("prior_rows_90d"), F.lit(0)), 3).alias("prior_claimed_amount_90d_per_prior_rows_90d"),
        F.round(F.col("holder_risk_score") * 1.5, 2).alias("holder_risk_score_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

profiled = (
    labeled.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.total_claim_approved_amount", "c.last_claim_date")
)

result = (
    profiled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_90d",
        "prior_claimed_amount_90d",
        "policy_start_date",
        "policy_premium",
        "policy_holder_id",
        "holder_joined_on",
        "holder_risk_score",
        "prior_rows_90d_filled",
        "prior_claimed_amount_90d_per_prior_rows_90d",
        "total_claim_approved_amount",
        "last_claim_date",
    )
    .orderBy("prior_claimed_amount_90d", F.col("policy_premium").desc())
)
