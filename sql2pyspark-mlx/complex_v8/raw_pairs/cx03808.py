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
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_children_claims = (
    joined_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_approved_amount_180d",
        "s.policy_status",
        "s.policy_premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        "c.claim_type",
    )
)

result = (
    with_children_claims
    .filter(F.col("claim_approved_amount") > 2272.7)
    .select(
        "id",
        "prior_approved_amount_180d",
        "policy_premium",
        "claim_id",
        "claim_approved_amount",
        "claim_claim_date",
        "claim_type",
        F.when(F.col("claim_type").isin("medical", "fire"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "claim_claim_date")
)
