from pyspark.sql import functions as F

matched = (
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

qualified = (
    matched.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") <= 4417.0), "left_anti")
)

screened = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (~F.col("p.status").isin("ACTIVE", "CANCELLED")) & (~F.col("p.product").isin("HOME", "HEALTH")), "left_anti")
)

without_matches = (
    screened.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
    .orderBy(F.col("id").asc(), F.col("policy_id").desc())
    .limit(20)
)
