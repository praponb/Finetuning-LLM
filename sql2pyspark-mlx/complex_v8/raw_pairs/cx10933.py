from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
    )
)

annotated = (
    base
    .filter(F.col("claim_type").isNotNull())
    .select(
        "*",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
)

joined = (
    annotated.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.premium").between(2033.8, 4670.9))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claim_type_prefix",
        F.col("p.premium").alias("policy_premium"),
        F.col("po.segment").alias("holder_segment"),
    )
)

result = (
    joined
    .cube("holder_segment", "claim_type", "adjuster_id")
    .agg(
        F.grouping("holder_segment").alias("is_total_holder_segment"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("holder_segment"), F.lit("ALL")).alias("holder_segment"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "adjuster_id",
        "is_total_holder_segment",
        "row_count",
        "distinct_ids",
    )
)
