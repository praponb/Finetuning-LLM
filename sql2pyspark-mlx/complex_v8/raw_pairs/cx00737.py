from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.claimed_amount").alias("prior_claimed_amount_180d"),
    )
)

with_details_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_claimed_amount_180d",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("po.joined_on").alias("holder_joined_on"),
        F.col("po.region").alias("holder_region"),
    )
)

line_level_claims = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_claimed_amount_180d",
        "s.policy_premium",
        "s.policy_status",
        "s.holder_joined_on",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

with_details = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_claimed_amount_180d",
        "s.policy_status",
        "s.claim_id",
        "s.claim_claim_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    with_details
    .select(
        "id",
        "claim_date",
        "prior_claimed_amount_180d",
        "policy_status",
        "claim_id",
        "claim_adjuster_id",
        "claim_approved_amount",
        "claim_claimed_amount",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy("id", "claim_claimed_amount")
)
