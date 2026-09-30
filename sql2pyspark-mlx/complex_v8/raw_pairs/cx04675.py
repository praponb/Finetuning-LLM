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
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

filtered = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_approved_amount_60d",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy(F.col("claim_date").asc(), F.col("policy_id").desc(), "id")
    .limit(10)
)
