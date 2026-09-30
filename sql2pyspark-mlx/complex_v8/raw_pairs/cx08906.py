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

rollup_base = (
    matched
    .groupBy(
        "policy_id",
        "prior_rows_180d",
        F.year("claim_date").alias("claim_date_year"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("prior_claimed_amount_180d"), 2).alias("total_prior_claimed_amount_180d"),
        F.sum("prior_claimed_amount_180d").alias("total_prior_claimed_amount_180d_2"),
        F.sum("prior_rows_180d").alias("total_prior_rows_180d"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") == "IN_REVIEW", 1)).alias("claim_status_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    rollup_base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.last_claim_reported_at",
    )
)

enriched = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.policy_id",
        "s.prior_rows_180d",
        "s.claim_date_year",
        "s.total_prior_claimed_amount_180d_2",
        "s.total_prior_rows_180d",
        "s.claim_status_count",
        "s.last_claim_reported_at",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    enriched
    .select(
        "policy_id",
        "prior_rows_180d",
        "claim_date_year",
        "total_prior_claimed_amount_180d_2",
        "policy_end_date",
    )
)
