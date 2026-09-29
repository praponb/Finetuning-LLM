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
        F.sum("b.claimed_amount").alias("prior_claimed_amount_90d"),
    )
)

derived = (
    covered
    .filter(F.col("policy_id") >= 5)
    .select(
        "*",
        F.when(F.col("prior_claimed_amount_90d") >= 175000.0, "large").when(F.col("prior_claimed_amount_90d") >= 62500.0, "medium").otherwise("small").alias("prior_claimed_amount_90d_tier"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
        F.add_months(F.col("claim_date"), 12).alias("claim_date_plus_12m"),
    )
)

totals = (
    derived
    .filter(F.col("prior_claimed_amount_90d") >= 112500.0)
    .groupBy(
        "prior_rows_90d",
        F.date_trunc("month", "claim_date_plus_12m").alias("claim_date_plus_12m_month"),
        "prior_claimed_amount_90d_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("prior_rows_90d").alias("max_prior_rows_90d"),
        F.min("prior_rows_90d").alias("min_prior_rows_90d"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.stddev("prior_claimed_amount_90d"), 2).alias("stddev_prior_claimed_amount_90d"),
        F.sum("prior_rows_90d").alias("sum_prior_rows_90d_check"),
    )
    .filter(F.col("sum_prior_rows_90d_check") > 2)
    .drop("sum_prior_rows_90d_check")
)

result = (
    totals
    .filter(F.col("min_prior_rows_90d") <= 5)
    .select(
        "prior_rows_90d",
        "claim_date_plus_12m_month",
        "prior_claimed_amount_90d_tier",
        "max_prior_rows_90d",
        "distinct_policy_ids",
        (F.col("max_prior_rows_90d") - F.col("min_prior_rows_90d")).alias("max_prior_rows_90d_minus_min_prior_rows_90d"),
    )
    .orderBy(F.col("prior_claimed_amount_90d_tier").asc())
)
