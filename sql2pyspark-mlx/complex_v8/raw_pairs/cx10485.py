from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

enriched = (
    cleaned_claims
    .filter(F.col("adjuster_id") <= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "claim_type_prefix",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type_prefix")), " ", "_")).alias("claim_type_prefix_slug"),
        F.concat(F.col("claim_type_prefix"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_prefix_ref"),
    )
)

result = (
    enriched
    .rollup("policy_id", "adjuster_id")
    .agg(
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
