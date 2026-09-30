from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-04-27").cast("date"), F.lit("2025-04-25").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

joined = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.product").alias("policy_product"),
    )
)

per_group = (
    joined
    .filter(F.col("policy_end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2027-01-03").cast("date")))
    .groupBy("policy_product", "claim_type_len")
    .agg(
        F.round(100 * F.count(F.when(F.col("policy_product").isin("TRAVEL", "HOME", "HEALTH"), 1)) / F.count("*"), 1).alias("pct_policy_product_in_travel_hom"),
        F.sum(F.when(F.col("policy_product").isin("HOME", "HEALTH", "AUTO"), F.col("claim_type_len")).otherwise(0)).alias("claim_type_len_policy_product_in_home_healt"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("claim_type_len"), F.lit(0)), 3).alias("claimed_amount_to_claim_type_len_ratio"),
        F.round(F.sum("claim_type_len") / F.nullif(F.sum("approved_amount"), F.lit(0)), 3).alias("claim_type_len_to_approved_amount_ratio"),
    )
    .filter(F.col("max_claimed_amount") > 15381.6)
)

result = (
    per_group
    .filter(F.col("claim_type_len") <= 12)
    .select(
        "policy_product",
        "claim_type_len",
        "pct_policy_product_in_travel_hom",
        "claim_type_len_policy_product_in_home_healt",
        "max_claimed_amount",
        "claimed_amount_to_claim_type_len_ratio",
        "claim_type_len_to_approved_amount_ratio",
        F.length(F.trim(F.col("policy_product"))).alias("policy_product_len"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
    )
    .orderBy("claim_type_len")
)
