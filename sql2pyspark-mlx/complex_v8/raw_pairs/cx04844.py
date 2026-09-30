from pyspark.sql import functions as F

joined_ranges = (
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

policy_id_stats = (
    joined_ranges
    .groupBy("policy_id")
    .agg(F.avg("prior_claimed_amount_90d").alias("avg_prior_claimed_amount_90d"))
)

vs_group = (
    joined_ranges.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_claimed_amount_90d") > F.col("g.avg_prior_claimed_amount_90d"))
    .select(
        "s.*",
        "g.avg_prior_claimed_amount_90d",
        F.round(F.col("s.prior_claimed_amount_90d") - F.col("g.avg_prior_claimed_amount_90d"), 2).alias("prior_claimed_amount_90d_minus_avg"),
    )
)

with_activity = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(1, 2)), "left_anti")
)

joined = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_90d",
        "s.prior_claimed_amount_90d_minus_avg",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    joined
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_90d",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
