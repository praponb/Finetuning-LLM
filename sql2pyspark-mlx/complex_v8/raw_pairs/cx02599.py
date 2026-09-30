from pyspark.sql import functions as F

joined_ranges = (
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
    joined_ranges
    .filter(F.col("prior_claimed_amount_180d") < 175000.0)
    .groupBy(
        F.year("claim_date").alias("claim_date_year"),
        "policy_id",
        "prior_rows_180d",
    )
    .agg(
        F.sum("prior_rows_180d").alias("total_prior_rows_180d"),
        F.min("prior_claimed_amount_180d").alias("min_prior_claimed_amount_180d"),
        F.round(F.stddev("prior_rows_180d"), 2).alias("stddev_prior_rows_180d"),
        F.round(F.stddev("prior_claimed_amount_180d"), 2).alias("stddev_prior_claimed_amount_180d"),
        F.avg("prior_claimed_amount_180d").alias("avg_prior_claimed_amount_180d_check"),
    )
    .filter(F.col("avg_prior_claimed_amount_180d_check") > 87500.0)
    .drop("avg_prior_claimed_amount_180d_check")
)

combined_details_policy = (
    stats.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.claim_date_year",
        "s.policy_id",
        "s.prior_rows_180d",
        "s.total_prior_rows_180d",
        "s.min_prior_claimed_amount_180d",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        "po.holder_name",
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    combined_details_policy
    .select(
        "claim_date_year",
        "policy_id",
        "total_prior_rows_180d",
        "prior_rows_180d",
    )
    .unpivot(
        ids=["claim_date_year", "policy_id"],
        values=["total_prior_rows_180d", "prior_rows_180d"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
