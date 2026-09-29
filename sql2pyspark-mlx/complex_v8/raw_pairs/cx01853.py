from pyspark.sql import functions as F

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

joined_policy = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_approved_amount_30d",
        F.col("p.status").alias("policy_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("claim_date").alias("last_claim_date"))
)

with_counts = (
    joined_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_date")
)

result = (
    with_counts
    .groupBy("claim_date", "last_claim_date")
    .pivot("policy_status", ["ACTIVE", "EXPIRED"])
    .agg(F.sum("prior_approved_amount_30d"))
    .orderBy("claim_date", "last_claim_date")
)
