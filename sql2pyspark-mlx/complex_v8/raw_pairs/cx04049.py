from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.approved_amount").alias("prior_approved_amount_180d"),
    )
)

joined_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_180d",
        "s.prior_approved_amount_180d",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_activity = (
    joined_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(10, 15)) & (F.col("c.status") != "APPROVED"), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.sum("approved_amount").alias("total_claim_approved_amount"))
)

with_counts = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.total_claim_approved_amount")
)

result = (
    with_counts
    .rollup("prior_rows_180d", "policy_id")
    .agg(
        F.round(F.stddev("prior_approved_amount_180d"), 2).alias("stddev_prior_approved_amount_180d"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("prior_rows_180d").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
