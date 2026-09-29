from pyspark.sql import functions as F

covered = (
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

joined_policy = (
    covered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isin("CANCELLED", "EXPIRED")), "inner")
    .filter(F.col("p.product") == "HEALTH")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_claimed_amount_180d",
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    joined_policy
    .groupBy("policy_id", "policy_start_date")
    .pivot("policy_status", ["ACTIVE", "EXPIRED"])
    .agg(F.min("prior_rows_180d"))
)
