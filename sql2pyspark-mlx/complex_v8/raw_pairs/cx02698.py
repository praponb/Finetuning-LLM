from pyspark.sql import functions as F

covered = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_90d"),
    )
)

with_children = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_rows_90d",
        "s.prior_approved_amount_90d",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
    )
)

with_details_policy = (
    with_children.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_rows_90d",
        "s.prior_approved_amount_90d",
        "s.claim_id",
        "s.claim_status",
        "s.claim_reported_at",
        "s.claim_type",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
    )
)

with_activity = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 39666.5) & (F.col("c.status") == "IN_REVIEW"), "left_anti")
    .filter(F.col("claim_type") == "medical")
)

result = (
    with_activity
    .select(
        "id",
        "policy_id",
        "prior_rows_90d",
        "prior_approved_amount_90d",
        "claim_id",
        "claim_status",
        "claim_reported_at",
    )
    .orderBy("policy_id", F.col("prior_approved_amount_90d").asc_nulls_last())
)
