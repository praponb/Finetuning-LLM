from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("approved_amount") > 9389.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    valid_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_stats
    .cube("status", "claim_claim_type_count", "adjuster_id")
    .agg(
        F.round(F.sum("claim_claim_type_count") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("claim_claim_type_count_to_claimed_amount_ratio"),
        F.count("*").alias("row_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("claim_claim_type_count").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
