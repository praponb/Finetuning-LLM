from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp")) & (F.col("policy_id") <= 10))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.when(F.col("claim_type").isin("collision", "medical"), "group_a").when(F.col("claim_type") == "flood", "group_b").otherwise("other").alias("claim_type_group"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.claim_date_quarter",
        "s.claim_type_group",
        "s.reported_at_date",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    enriched
    .cube("claim_type_group", "claim_type", "adjuster_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("claim_type_group"), F.lit("ALL")).alias("claim_type_group"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "adjuster_id",
        "grouping_level",
        "total_claimed_amount",
    )
    .orderBy(
        F.col("claim_type_group").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
