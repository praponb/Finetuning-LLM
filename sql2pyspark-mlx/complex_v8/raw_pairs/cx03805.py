from pyspark.sql import functions as F

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_60d"),
    )
)

with_parent = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_claimed_amount_60d",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "policy_id",
        "claim_date",
        "policy_premium",
        "policy_end_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("policy_end_date")).alias("days_since_policy_end_date"),
    )
    .orderBy(F.col("id").desc(), F.col("policy_end_date").desc())
    .limit(5)
)
