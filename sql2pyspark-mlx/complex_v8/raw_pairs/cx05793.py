from pyspark.sql import functions as F

prepared_claims = df_claims.filter(F.col("policy_id") <= 17)

combined_details = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

exploded_detail = (
    combined_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.policy_start_date",
        "s.policy_product",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    exploded_detail
    .cube("claim_claim_type", "claim_id", "policy_product")
    .agg(F.count("*").alias("cnt"))
    .orderBy(
        F.col("claim_claim_type").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("policy_product").asc_nulls_last(),
    )
)
