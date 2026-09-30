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
        F.sum("b.claimed_amount").alias("prior_claimed_amount_30d"),
    )
)

qualified = (
    covered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id") <= 2) & (F.col("c.claimed_amount") < 31752.9), "left_semi")
)

policy_id_stats = (
    qualified
    .groupBy("policy_id")
    .agg(F.percentile_approx("prior_claimed_amount_30d", 0.5).alias("median_prior_claimed_amount_30d"))
)

vs_group = (
    qualified.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_claimed_amount_30d") >= F.col("g.median_prior_claimed_amount_30d"))
    .select(
        "s.*",
        "g.median_prior_claimed_amount_30d",
        F.round(F.col("s.prior_claimed_amount_30d") - F.col("g.median_prior_claimed_amount_30d"), 2).alias("prior_claimed_amount_30d_minus_median"),
    )
)

result = (
    vs_group
    .select("id", "policy_id", "prior_rows_30d")
    .orderBy(F.col("policy_id").desc(), "id")
)
