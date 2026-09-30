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

derived = matched.select("*", F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"))

policy_id_stats = (
    derived
    .groupBy("policy_id")
    .agg(F.percentile_approx("prior_rows_90d", 0.5).alias("median_prior_rows_90d"))
)

compared = (
    derived.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_90d") >= F.col("g.median_prior_rows_90d"))
    .select(
        "s.*",
        "g.median_prior_rows_90d",
        F.round(F.col("s.prior_rows_90d") - F.col("g.median_prior_rows_90d"), 2).alias("prior_rows_90d_minus_median"),
    )
)

result = (
    compared
    .filter(F.col("median_prior_rows_90d") >= 2.4)
    .select(
        "id",
        "claim_date",
        "prior_approved_amount_90d",
        "claim_date_week",
        "median_prior_rows_90d",
        "prior_rows_90d_minus_median",
        F.coalesce(F.col("prior_rows_90d"), F.lit(0)).alias("prior_rows_90d_filled"),
    )
)
