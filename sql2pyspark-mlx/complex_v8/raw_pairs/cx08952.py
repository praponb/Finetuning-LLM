from pyspark.sql import functions as F

valid = df_claims.filter((F.col("policy_id") <= 27) | (F.col("claimed_amount") <= 32441.2))

annotated = (
    valid
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

result = (
    annotated
    .groupingSets(
        [["claim_type_or_unknown", "policy_id", "claim_type"], ["claim_type_or_unknown"], ["policy_id"], []],
        "claim_type_or_unknown", "policy_id", "claim_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("claimed_amount_adj") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("claimed_amount_adj_to_claimed_amount_ratio"),
    )
    .select(
        F.coalesce(F.col("claim_type_or_unknown"), F.lit("ALL")).alias("claim_type_or_unknown"),
        "policy_id",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "gid",
        "claimed_amount_adj_to_claimed_amount_ratio",
    )
    .orderBy(
        F.col("claim_type_or_unknown").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
