from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 180)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_180d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_180d"),
    )
)

with_parent_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_approved_amount_180d",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

qualified = (
    with_parent_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") < F.lit("2025-01-10").cast("date")) & (F.col("c.claim_type") == "collision"), "left_semi")
    .filter(F.col("holder_segment") == "SME")
)

result = (
    qualified
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_180d",
        "prior_approved_amount_180d",
        "policy_start_date",
        "policy_premium",
        "holder_joined_on",
        F.round(F.col("policy_premium") * 0.25, 2).alias("policy_premium_adj"),
    )
    .orderBy("policy_id", F.col("prior_approved_amount_180d").desc(), "id")
    .limit(10)
)
