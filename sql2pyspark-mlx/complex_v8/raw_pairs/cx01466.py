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

rollup_base = (
    covered
    .groupBy(
        "policy_id",
        F.year("claim_date").alias("claim_date_year"),
        "prior_rows_60d",
    )
    .agg(
        F.round(F.sum("prior_approved_amount_60d"), 2).alias("total_prior_approved_amount_60d"),
        F.round(F.sum("prior_rows_60d") / F.nullif(F.sum("prior_approved_amount_60d"), F.lit(0)), 3).alias("prior_rows_60d_to_prior_approved_amount_60d_rati"),
        F.min("claim_date").alias("first_claim_date"),
        F.round(F.avg("prior_approved_amount_60d"), 2).alias("avg_prior_approved_amount_60d"),
        F.round(F.stddev("prior_rows_60d"), 2).alias("stddev_prior_rows_60d"),
        F.sum("prior_rows_60d").alias("sum_prior_rows_60d_check"),
    )
    .filter(F.col("sum_prior_rows_60d_check") > 1)
    .drop("sum_prior_rows_60d_check")
)

detail_rows_claims = (
    rollup_base.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.policy_id",
        "s.claim_date_year",
        "s.prior_rows_60d",
        "s.total_prior_approved_amount_60d",
        "s.stddev_prior_rows_60d",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

enriched = (
    detail_rows_claims
    .filter(F.col("claim_claimed_amount") < 40638.4)
    .select(
        "*",
        F.greatest(F.col("claim_date_year"), F.col("claim_claimed_amount")).alias("greatest_claim_date_year_claim_claimed_amount"),
        F.date_trunc("week", F.col("claim_reported_at")).alias("claim_reported_at_week"),
    )
)

result = (
    enriched
    .select(
        "policy_id",
        "claim_date_year",
        "prior_rows_60d",
        "total_prior_approved_amount_60d",
        "stddev_prior_rows_60d",
        "claim_claimed_amount",
        "claim_approved_amount",
        "greatest_claim_date_year_claim_claimed_amount",
        "claim_reported_at_week",
    )
    .orderBy("stddev_prior_rows_60d")
)
