from pyspark.sql import functions as F

matched = (
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

stats = (
    matched
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "prior_rows_180d",
        "policy_id",
    )
    .agg(
        F.round(F.stddev("prior_claimed_amount_180d"), 2).alias("stddev_prior_claimed_amount_180d"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.percentile_approx("prior_rows_180d", 0.5).alias("median_prior_rows_180d"),
        F.min("claim_date").alias("first_claim_date"),
        F.round(F.stddev("prior_rows_180d"), 2).alias("stddev_prior_rows_180d"),
    )
)

scored = (
    stats
    .withColumn("median_prior_rows_180d_per_prior_rows_180d", F.round(F.col("median_prior_rows_180d") / F.nullif(F.col("prior_rows_180d"), F.lit(0)), 3))
)

result = (
    scored
    .filter(F.col("median_prior_rows_180d_per_prior_rows_180d") < 8.5)
    .select(
        "claim_date_month",
        "policy_id",
        "stddev_prior_claimed_amount_180d",
        "claim_date_span_days",
        "median_prior_rows_180d",
        "first_claim_date",
        "stddev_prior_rows_180d",
        "median_prior_rows_180d_per_prior_rows_180d",
        F.to_date(F.col("claim_date_month")).alias("claim_date_month_date"),
    )
    .orderBy(F.col("policy_id").desc())
)
