from pyspark.sql import functions as F

joined_ranges = (
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

with_parent_policy = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    with_parent_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "policy_premium",
        "policy_start_date",
        F.date_add(F.col("policy_start_date"), 365).alias("policy_start_date_plus_365d"),
        F.greatest(F.col("prior_claimed_amount_60d"), F.col("prior_rows_60d")).alias("greatest_prior_claimed_amount_60d_prior_rows_60d"),
    )
    .orderBy(F.col("policy_premium").asc_nulls_last())
)
