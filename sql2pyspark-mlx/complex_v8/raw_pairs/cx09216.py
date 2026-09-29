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
        F.sum("b.approved_amount").alias("prior_approved_amount_90d"),
    )
)

joined_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    joined_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "policy_end_date",
        "policy_status",
        F.datediff(F.col("claim_date"), F.col("policy_end_date")).alias("days_policy_end_date_to_claim_date"),
        F.datediff(F.col("policy_end_date"), F.col("claim_date")).alias("days_claim_date_to_policy_end_date"),
    )
    .orderBy(
        F.col("days_policy_end_date_to_claim_date").asc(),
        F.col("claim_date").asc(),
    )
)
