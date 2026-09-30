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

combined_details_policy = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_90d",
        "s.prior_claimed_amount_90d",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

labeled = (
    combined_details_policy
    .filter(F.col("policy_id").isNotNull())
    .select("*", F.year(F.col("policy_end_date")).alias("policy_end_date_year"))
)

result = (
    labeled
    .cube("policy_id", "policy_end_date_year")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("claim_date").alias("first_claim_date"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("policy_end_date_year").asc_nulls_last(),
    )
)
